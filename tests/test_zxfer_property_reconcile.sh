#!/bin/sh
#
# shunit2 tests for zxfer_property_cache.sh and zxfer_property_reconcile.sh helpers.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_transfer_props"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
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
	g_test_base_readonly_properties="readonly,mountpoint"
	g_test_freebsd_readonly_properties="aclmode"
	g_test_solexp_readonly_properties="jailed"
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
	g_option_T_target_host=""
	g_target_cmd_zfs=""
	g_cmd_ssh=$(command -v ssh 2>/dev/null || printf '%s\n' ssh)
	unset FAKE_REMOTE_PATH
	unset FAKE_SSH_LOG
	unset ZXFER_REMOTE_ZFS_LOG
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
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=0
	g_unsupported_properties=""
	g_zxfer_unsupported_filesystem_properties=""
	g_zxfer_unsupported_volume_properties=""
	g_zxfer_property_stage_file_read_result=""
	zxfer_get_base_readonly_properties() {
		printf '%s\n' "$g_test_base_readonly_properties"
	}
	zxfer_get_freebsd_readonly_properties() {
		printf '%s\n' "$g_test_freebsd_readonly_properties"
	}
	zxfer_get_solexp_readonly_properties() {
		printf '%s\n' "$g_test_solexp_readonly_properties"
	}
	zxfer_reset_failure_context "unit"
}

test_readonly_property_constant_getters_return_source_constants() {
	result=$(
		(
			# shellcheck source=src/zxfer_property_reconcile.sh
			. "$TESTS_DIR/../src/zxfer_property_reconcile.sh"
			printf 'base=%s\n' "$(zxfer_get_base_readonly_properties)"
			printf 'freebsd=%s\n' "$(zxfer_get_freebsd_readonly_properties)"
			printf 'solexp=%s\n' "$(zxfer_get_solexp_readonly_properties)"
		)
	)

	assertContains "The base readonly getter should return the source-time constant list." \
		"$result" "base=type,creation,used,available"
	assertContains "The FreeBSD readonly getter should return the platform constant list." \
		"$result" "freebsd=aclmode,aclinherit,devices,nbmand,shareiscsi,vscan,xattr,dnodesize"
	assertContains "The Solaris Express readonly getter should return the compatibility constant list." \
		"$result" "solexp=jailed,aclmode,shareiscsi"
}

test_get_effective_readonly_properties_removes_mountpoint_during_migration() {
	result=$(
		(
			g_option_m_migrate=1
			g_destination_operating_system="FreeBSD"
			# shellcheck source=src/zxfer_property_reconcile.sh
			. "$TESTS_DIR/../src/zxfer_property_reconcile.sh"
			zxfer_get_effective_readonly_properties
		)
	)

	assertNotContains "Migration should remove mountpoint from the effective readonly list." \
		"$result" "mountpoint"
	assertContains "Migration should still preserve the base readonly list." \
		"$result" "type,creation"
	assertContains "FreeBSD destinations should still append the platform readonly properties." \
		"$result" "aclmode,aclinherit,devices,nbmand,shareiscsi,vscan,xattr,dnodesize"
}

test_get_effective_readonly_properties_uses_freebsd_list_when_base_is_empty() {
	result=$(
		(
			g_destination_operating_system="FreeBSD"
			# shellcheck source=src/zxfer_property_reconcile.sh
			. "$TESTS_DIR/../src/zxfer_property_reconcile.sh"
			zxfer_get_base_readonly_properties() {
				printf '\n'
			}
			zxfer_get_effective_readonly_properties
		)
	)

	assertEquals "An empty base list should still allow the FreeBSD readonly list to become the full effective list." \
		"aclmode,aclinherit,devices,nbmand,shareiscsi,vscan,xattr,dnodesize" "$result"
}

test_get_effective_readonly_properties_uses_solexp_list_when_base_is_empty() {
	result=$(
		(
			g_destination_operating_system="SunOS"
			g_source_operating_system="FreeBSD"
			# shellcheck source=src/zxfer_property_reconcile.sh
			. "$TESTS_DIR/../src/zxfer_property_reconcile.sh"
			zxfer_get_base_readonly_properties() {
				printf '\n'
			}
			zxfer_get_effective_readonly_properties
		)
	)

	assertEquals "An empty base list should still allow the Solaris Express compatibility list to become the full effective list." \
		"jailed,aclmode,shareiscsi" "$result"
}

test_zxfer_property_reconcile_state_helpers_cover_current_shell_paths() {
	stage_file="$TEST_TMPDIR/property_reconcile_stage_file.out"
	stage_read_output_file="$TEST_TMPDIR/property_reconcile_stage_read.out"
	printf '%s\n' "compression=lz4=local" >"$stage_file"

	g_unsupported_properties="compression"
	g_zxfer_unsupported_filesystem_properties="compression"
	g_zxfer_unsupported_volume_properties="volblocksize"
	zxfer_reset_property_runtime_state

	g_zxfer_new_rmvs_pv="stale-remove-sources"
	g_zxfer_new_rmv_pvs="stale-remove"
	g_zxfer_new_mc_pvs="stale-select"
	g_zxfer_only_supported_properties="stale-supported"
	g_zxfer_adjusted_set_list="stale-set"
	g_zxfer_adjusted_inherit_list="stale-inherit"
	g_zxfer_source_pvs_raw="stale-raw"
	g_zxfer_source_pvs_effective="stale-effective"
	g_zxfer_override_pvs_result="stale-override"
	g_zxfer_creation_pvs_result="stale-create"
	g_zxfer_property_stage_file_read_result="stale-stage"
	zxfer_reset_property_reconcile_state
	zxfer_read_property_reconcile_stage_file "$stage_file" >"$stage_read_output_file"
	read_output=$(cat "$stage_read_output_file")

	assertEquals "Resetting property runtime state should clear the merged unsupported-property list." \
		"" "$g_unsupported_properties"
	assertEquals "Resetting property runtime state should clear the filesystem unsupported-property cache." \
		"" "$g_zxfer_unsupported_filesystem_properties"
	assertEquals "Resetting property runtime state should clear the volume unsupported-property cache." \
		"" "$g_zxfer_unsupported_volume_properties"
	assertEquals "Resetting property reconcile state should clear the remove-sources scratch list." \
		"" "$g_zxfer_new_rmvs_pv"
	assertEquals "Resetting property reconcile state should clear the remove-properties scratch list." \
		"" "$g_zxfer_new_rmv_pvs"
	assertEquals "Resetting property reconcile state should clear the select-properties scratch list." \
		"" "$g_zxfer_new_mc_pvs"
	assertEquals "Resetting property reconcile state should clear the supported-properties scratch list." \
		"" "$g_zxfer_only_supported_properties"
	assertEquals "Resetting property reconcile state should clear the adjusted set-list scratch state." \
		"" "$g_zxfer_adjusted_set_list"
	assertEquals "Resetting property reconcile state should clear the adjusted inherit-list scratch state." \
		"" "$g_zxfer_adjusted_inherit_list"
	assertEquals "Resetting property reconcile state should clear the source property raw scratch state." \
		"" "$g_zxfer_source_pvs_raw"
	assertEquals "Resetting property reconcile state should clear the source property effective scratch state." \
		"" "$g_zxfer_source_pvs_effective"
	assertEquals "Resetting property reconcile state should clear the override-property scratch state." \
		"" "$g_zxfer_override_pvs_result"
	assertEquals "Resetting property reconcile state should clear the creation-property scratch state." \
		"" "$g_zxfer_creation_pvs_result"
	assertEquals "Property reconcile stage-file reads should trim the trailing newline from staged output." \
		"compression=lz4=local" "$read_output"
	assertEquals "Property reconcile stage-file reads should publish the staged payload in shared scratch state." \
		"compression=lz4=local" "$g_zxfer_property_stage_file_read_result"
}

test_get_effective_readonly_properties_appends_platform_lists_when_base_is_nonempty() {
	g_destination_operating_system="FreeBSD"
	freebsd_result=$(zxfer_get_effective_readonly_properties)
	sunos_result=$(
		(
			g_destination_operating_system="SunOS"
			g_source_operating_system="FreeBSD"
			zxfer_get_effective_readonly_properties
		)
	)

	assertContains "Effective readonly-property resolution should append the FreeBSD compatibility list to the base list when the base list is already populated." \
		"$freebsd_result" "readonly,mountpoint,aclmode"
	assertContains "Effective readonly-property resolution should append the Solaris Express compatibility list to the base list when the base list is already populated." \
		"$sunos_result" "readonly,mountpoint,jailed"
}

test_unsupported_property_probe_helpers_cover_current_shell_paths() {
	output=$(
		(
			zxfer_exists_destination() {
				if [ "$1" = "backup/dst/child" ]; then
					printf '%s\n' 0
				else
					printf '%s\n' 1
				fi
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "filesystem"
			}

			probe_destination=$(zxfer_get_unsupported_property_probe_destination_for_source "tank/src/child")
			probe_dataset=$(zxfer_get_unsupported_property_probe_dataset "$probe_destination")
			probe_type=$(zxfer_get_unsupported_property_probe_dataset_type "backup")
			zxfer_append_unsupported_property_for_dataset_type filesystem "compression"
			zxfer_append_unsupported_property_for_dataset_type filesystem "compression"
			zxfer_append_unsupported_property_for_dataset_type volume "volblocksize"
			zxfer_select_unsupported_properties_for_dataset_type filesystem
			printf 'destination=%s\n' "$probe_destination"
			printf 'dataset=%s\n' "$probe_dataset"
			printf 'type=%s\n' "$probe_type"
			printf 'filesystem=%s\n' "$g_zxfer_unsupported_filesystem_properties"
			printf 'volume=%s\n' "$g_zxfer_unsupported_volume_properties"
			printf 'selected=%s\n' "$g_unsupported_properties"
		)
	)

	assertContains "Unsupported-property probe destination mapping should preserve the source-relative destination path for descendants." \
		"$output" "destination=backup/dst/src/child"
	assertContains "Unsupported-property probe dataset selection should fall back to the destination pool when the requested descendant does not exist yet." \
		"$output" "dataset=backup"
	assertContains "Unsupported-property probe dataset-type detection should report the destination dataset type." \
		"$output" "type=filesystem"
	assertContains "Unsupported-property accumulation should keep unique filesystem properties only once." \
		"$output" "filesystem=compression"
	assertContains "Unsupported-property accumulation should track volume-only properties separately." \
		"$output" "volume=volblocksize"
	assertContains "Unsupported-property selection should publish the filesystem list for filesystem datasets." \
		"$output" "selected=compression"
}

test_select_mc_picks_requested_properties() {
	l_oldifs=$IFS
	IFS=","
	zxfer_select_mc "casesensitivity=mixed=local,compression=lz4=local,utf8only=on=local" "utf8only,casesensitivity"
	IFS=$l_oldifs

	assertEquals "Must-create selection should preserve only the requested properties." \
		"casesensitivity=mixed=local,utf8only=on=local" "$g_zxfer_new_mc_pvs"
}

test_remove_properties_preserves_override_entries() {
	l_oldifs=$IFS
	IFS=","
	zxfer_remove_properties "mountpoint=/mnt=local,readonly=off=override,compression=lz4=local" "readonly,mountpoint"
	IFS=$l_oldifs

	assertEquals "Override entries should survive property filtering even when the property is listed for removal." \
		"readonly=off=override,compression=lz4=local" "$g_zxfer_new_rmv_pvs"
}

test_run_zfs_create_with_properties_executes_live_create() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$*"
			}
			zxfer_run_zfs_create_with_properties yes volume 10G "compression=lz4,atime=off" "backup/dst"
		)
	)

	assertEquals "Live zfs create should pass each argument separately." \
		"create -p -V 10G -o compression=lz4 -o atime=off backup/dst" "$result"
}

test_run_zfs_create_with_properties_renders_dry_run_command() {
	g_option_n_dryrun=1
	g_RZFS="/usr/bin/ssh 'host' /sbin/zfs"

	result=$(zxfer_run_zfs_create_with_properties no filesystem "" "compression=lz4,quota=1G" "backup/dst")

	assertEquals "Dry-run zfs create should render a safely quoted command line." \
		"/usr/bin/ssh 'host' /sbin/zfs 'create' '-o' 'compression=lz4' '-o' 'quota=1G' 'backup/dst'" "$result"
}

test_run_zfs_create_with_properties_rejects_volume_without_size() {
	set +e
	zxfer_run_zfs_create_with_properties yes volume "" "compression=lz4" "backup/dst" >/dev/null 2>&1
	status=$?

	assertEquals "Volume creates should fail closed when the source volsize is unavailable." 1 "$status"
}

test_run_zfs_create_with_properties_decodes_delimiter_heavy_assignments_for_exec() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$*"
			}
			zxfer_run_zfs_create_with_properties no filesystem "" \
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
			zxfer_run_zfs_cmd_for_spec() {
				if [ "$3" = "-Hpo" ]; then
					printf 'quota\t1073741824\tlocal\ncompression\tlz4\tlocal\n'
				else
					printf 'quota\tnone\tlocal\ncompression\tlz4\tlocal\n'
				fi
			}
			g_LZFS="/remote/zfs"
			zxfer_get_normalized_dataset_properties "tank/src" ""
		)
	)

	assertEquals "Normalized property lookup should merge machine and human values, preserving human none values." \
		"quota=none=local,compression=lz4=local" "$result"
}

test_get_normalized_dataset_properties_escapes_delimiter_heavy_values() {
	result=$(
		(
			zxfer_run_zfs_cmd_for_spec() {
				printf 'user:note\tvalue,=; mix\tlocal\n'
			}
			zxfer_get_normalized_dataset_properties "tank/src" "/sbin/zfs"
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
		zxfer_run_zfs_cmd_for_spec() {
			if [ "$3" = "-Hpo" ]; then
				printf 'compression\tlz4\tlocal\n'
			else
				printf 'compression\tlz4\tlocal\n'
			fi
		}
		zxfer_get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >/dev/null
		zxfer_get_normalized_dataset_properties "backup/dst" "/sbin/zfs" destination >/dev/null
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

	zxfer_run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		if [ "$3" = "-Hpo" ]; then
			printf 'compression\tlz4\tlocal\n'
		else
			printf 'compression\tlz4\tlocal\n'
		fi
	}

	zxfer_get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >"$log"
	zxfer_get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >>"$log"
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

	zxfer_run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		if [ "$3" = "-Hpo" ]; then
			printf 'compression\tlz4\tlocal\n'
		else
			printf 'compression\tlz4\tlocal\n'
		fi
	}

	zxfer_get_normalized_dataset_properties "tank/shared" "/sbin/zfs" source >"$log"
	zxfer_get_normalized_dataset_properties "tank/shared" "/sbin/zfs" destination >>"$log"
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

	zxfer_run_zfs_cmd_for_spec() {
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
	zxfer_get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >"$err_log" 2>&1
	status=$?

	l_mode="success"
	zxfer_get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >"$out_log"
	zxfer_property_cache_dataset_path normalized source "tank/src" >/dev/null
	cache_path=$g_zxfer_property_cache_path

	unset -f zxfer_run_zfs_cmd_for_spec

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

test_get_normalized_dataset_properties_reports_machine_serializer_failures_without_caching() {
	err_log="$TEST_TMPDIR/normalized_machine_serializer_failure.err"
	calls_log="$TEST_TMPDIR/normalized_machine_serializer_failure.calls"
	: >"$calls_log"

	zxfer_run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		printf 'compression\tlz4\tlocal\n'
	}

	zxfer_serialize_property_records_from_stdin() {
		printf '%s\n' "machine serializer failed" >&2
		return 23
	}

	set +e
	zxfer_get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >"$err_log" 2>&1
	status=$?
	zxfer_property_cache_dataset_path normalized source "tank/src" >/dev/null
	cache_path=$g_zxfer_property_cache_path

	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f zxfer_serialize_property_records_from_stdin
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_property_reconcile.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertNotEquals "Machine-side serializer failures should return a non-zero status." \
		"0" "$status"
	assertEquals "Machine-side serializer failures should surface the serializer stderr." \
		"machine serializer failed" "$(cat "$err_log")"
	assertEquals "Machine-side serializer failures should stop before the human probe runs." \
		"1" "$(awk 'END {print NR + 0}' "$calls_log")"
	assertFalse "Machine-side serializer failures should not populate the normalized-property cache." \
		"[ -e \"$cache_path\" ]"
}

test_get_normalized_dataset_properties_reports_machine_serializer_readback_failures_without_caching() {
	err_log="$TEST_TMPDIR/normalized_machine_serializer_readback_failure.err"
	calls_log="$TEST_TMPDIR/normalized_machine_serializer_readback_failure.calls"
	staged_output_file="$TEST_TMPDIR/normalized_machine_serializer_readback_failure.stage"
	: >"$calls_log"

	zxfer_get_temp_file() {
		g_zxfer_temp_file_result="$staged_output_file"
		: >"$g_zxfer_temp_file_result"
	}

	zxfer_run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		printf 'compression\tlz4\tlocal\n'
	}

	cat() {
		if [ "$1" = "$staged_output_file" ]; then
			printf '%s\n' "machine serializer readback failed" >&2
			printf '%s\n' "compression=lz4=local"
			return 26
		fi
		command cat "$@"
	}

	set +e
	zxfer_get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >"$err_log" 2>&1
	status=$?
	zxfer_property_cache_dataset_path normalized source "tank/src" >/dev/null
	cache_path=$g_zxfer_property_cache_path

	unset -f zxfer_get_temp_file
	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f cat
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_property_reconcile.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertEquals "Machine-side serializer readback failures should return the staged readback status." \
		"26" "$status"
	assertEquals "Machine-side serializer readback failures should surface the staged readback diagnostic." \
		"machine serializer readback failed" "$(cat "$err_log")"
	assertEquals "Machine-side serializer readback failures should stop before the human probe runs." \
		"1" "$(awk 'END {print NR + 0}' "$calls_log")"
	assertFalse "Machine-side serializer readback failures should not populate the normalized-property cache." \
		"[ -e \"$cache_path\" ]"
}

test_get_normalized_dataset_properties_reports_human_probe_failures_without_caching() {
	err_log="$TEST_TMPDIR/normalized_human_failure.err"
	calls_log="$TEST_TMPDIR/normalized_human_failure.calls"
	: >"$calls_log"

	zxfer_run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		if [ "$3" = "-Hpo" ]; then
			printf 'compression\tlz4\tlocal\n'
			return 0
		fi
		printf '%s\n' "ssh timeout"
		return 1
	}

	set +e
	zxfer_get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >"$err_log" 2>&1
	status=$?
	zxfer_property_cache_dataset_path normalized source "tank/src" >/dev/null
	cache_path=$g_zxfer_property_cache_path

	unset -f zxfer_run_zfs_cmd_for_spec

	assertEquals "Human normalized-property probe failures should return a non-zero status." \
		"1" "$status"
	assertEquals "Human normalized-property probe failures should surface the underlying zfs error." \
		"ssh timeout" "$(cat "$err_log")"
	assertEquals "Human normalized-property probe failures should still execute both normalized probes before failing." \
		"2" "$(awk 'END {print NR + 0}' "$calls_log")"
	assertFalse "Human normalized-property probe failures should not populate the cache." \
		"[ -e \"$cache_path\" ]"
}

test_get_normalized_dataset_properties_reports_human_serializer_failures_without_caching() {
	err_log="$TEST_TMPDIR/normalized_human_serializer_failure.err"
	calls_log="$TEST_TMPDIR/normalized_human_serializer_failure.calls"
	marker_file="$TEST_TMPDIR/normalized_human_serializer_failure.marker"
	: >"$calls_log"

	zxfer_run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		printf 'compression\tlz4\tlocal\n'
	}

	zxfer_serialize_property_records_from_stdin() {
		if [ -f "$marker_file" ]; then
			printf '%s\n' "human serializer failed" >&2
			return 24
		fi
		: >"$marker_file"
		printf '%s\n' "compression=lz4=local"
	}

	set +e
	zxfer_get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >"$err_log" 2>&1
	status=$?
	zxfer_property_cache_dataset_path normalized source "tank/src" >/dev/null
	cache_path=$g_zxfer_property_cache_path

	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f zxfer_serialize_property_records_from_stdin
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_property_reconcile.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertNotEquals "Human-side serializer failures should return a non-zero status." \
		"0" "$status"
	assertEquals "Human-side serializer failures should surface the serializer stderr." \
		"human serializer failed" "$(cat "$err_log")"
	assertEquals "Human-side serializer failures should still execute both normalized-property probes." \
		"2" "$(awk 'END {print NR + 0}' "$calls_log")"
	assertFalse "Human-side serializer failures should not populate the normalized-property cache." \
		"[ -e \"$cache_path\" ]"
}

test_load_normalized_dataset_properties_uses_prefetched_cache_entry_when_available() {
	calls_log="$TEST_TMPDIR/normalized_prefetch_cache.calls"
	cache_path="$TEST_TMPDIR/normalized_prefetch_cache.entry"
	: >"$calls_log"
	output=$(
		(
			CALLS_LOG="$calls_log"
			CACHE_PATH="$cache_path"
			zxfer_property_cache_dataset_path() {
				g_zxfer_property_cache_path=$CACHE_PATH
				printf '%s\n' "$g_zxfer_property_cache_path"
			}
			zxfer_maybe_prefetch_recursive_normalized_properties() {
				zxfer_write_cache_object_file_atomically \
					"$CACHE_PATH" \
					"$ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED" \
					"" \
					"compression=lz4=local" >/dev/null || return 1
				return 0
			}
			zxfer_run_zfs_cmd_for_spec() {
				printf 'call\n' >>"$CALLS_LOG"
				printf '%s\n' "unexpected live lookup"
				return 1
			}
			zxfer_load_normalized_dataset_properties "tank/src" "/sbin/zfs" source || exit $?
			printf 'props=%s\ncache_hit=%s\ncalls=%s\n' \
				"$g_zxfer_normalized_dataset_properties" \
				"${g_zxfer_normalized_dataset_properties_cache_hit:-0}" \
				"$(awk 'END {print NR + 0}' "$CALLS_LOG")"
		)
	)

	assertEquals "Normalized-property loads should reuse a prefetched cache entry when recursive prefetch materializes the dataset." \
		"props=compression=lz4=local
cache_hit=1
calls=0" "$output"
}

test_load_normalized_dataset_properties_falls_back_to_live_probe_when_cache_read_fails() {
	calls_log="$TEST_TMPDIR/normalized_cache_read_failure.calls"
	cache_path="$TEST_TMPDIR/normalized_cache_read_failure.entry"
	: >"$calls_log"
	printf '%s\n' "compression=stale=local" >"$cache_path"

	output=$(
		(
			CALLS_LOG="$calls_log"
			CACHE_PATH="$cache_path"
			zxfer_property_cache_dataset_path() {
				g_zxfer_property_cache_path=$CACHE_PATH
				printf '%s\n' "$g_zxfer_property_cache_path"
			}
			zxfer_read_property_cache_file() {
				return 1
			}
			zxfer_run_zfs_cmd_for_spec() {
				printf 'call\n' >>"$CALLS_LOG"
				printf 'compression\tlz4\tlocal\n'
			}
			zxfer_load_normalized_dataset_properties "tank/src" "/sbin/zfs" source || exit $?
			printf 'props=%s\ncache_hit=%s\ncalls=%s\n' \
				"$g_zxfer_normalized_dataset_properties" \
				"${g_zxfer_normalized_dataset_properties_cache_hit:-0}" \
				"$(awk 'END {print NR + 0}' "$CALLS_LOG")"
		)
	)

	assertEquals "Unreadable normalized-property cache entries should be treated as cache misses and retried live." \
		"props=compression=lz4=local
cache_hit=0
calls=2" "$output"
}

test_zxfer_read_property_cache_file_rejects_truncated_cache_objects() {
	cache_path="$TEST_TMPDIR/property_cache_partial_read.entry"
	cat >"$cache_path" <<-EOF
		$ZXFER_CACHE_OBJECT_HEADER_LINE
		kind=$ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED

		compression=partial=local
	EOF

	set +e
	output=$(
		(
			CACHE_PATH="$cache_path"
			zxfer_read_property_cache_file \
				"$CACHE_PATH" "$ZXFER_PROPERTY_CACHE_OBJECT_KIND_NORMALIZED"
			l_status=$?
			printf 'status=%s\n' "$l_status"
			printf 'result=<%s>\n' "${g_zxfer_property_cache_read_result:-}"
		)
	)
	status=$?

	assertEquals "Direct property-cache read tests should complete the subshell cleanly." \
		0 "$status"
	assertEquals "Property-cache reads should fail closed when the cache object is truncated." \
		"status=1
result=<>" "$output"
}

test_load_normalized_dataset_properties_falls_back_to_live_probe_when_cache_read_returns_partial_failure() {
	calls_log="$TEST_TMPDIR/normalized_cache_partial_read_failure.calls"
	cache_path="$TEST_TMPDIR/normalized_cache_partial_read_failure.entry"
	: >"$calls_log"
	printf '%s\n' "compression=stale=local" >"$cache_path"

	output=$(
		(
			CALLS_LOG="$calls_log"
			CACHE_PATH="$cache_path"
			zxfer_property_cache_dataset_path() {
				g_zxfer_property_cache_path=$CACHE_PATH
				printf '%s\n' "$g_zxfer_property_cache_path"
			}
			zxfer_read_property_cache_file() {
				g_zxfer_property_cache_read_result="compression=stale=local"
				return 9
			}
			zxfer_run_zfs_cmd_for_spec() {
				printf 'call\n' >>"$CALLS_LOG"
				printf 'compression\tlz4\tlocal\n'
			}
			zxfer_load_normalized_dataset_properties "tank/src" "/sbin/zfs" source || exit $?
			printf 'props=%s\ncache_hit=%s\ncalls=%s\n' \
				"$g_zxfer_normalized_dataset_properties" \
				"${g_zxfer_normalized_dataset_properties_cache_hit:-0}" \
				"$(awk 'END {print NR + 0}' "$CALLS_LOG")"
		)
	)

	assertEquals "Partial normalized-property cache reads that fail nonzero should still be treated as cache misses and retried live." \
		"props=compression=lz4=local
cache_hit=0
calls=2" "$output"
}

test_load_normalized_dataset_properties_falls_back_to_live_probe_when_prefetched_cache_entry_is_empty() {
	calls_log="$TEST_TMPDIR/normalized_prefetch_empty_cache.calls"
	cache_path="$TEST_TMPDIR/normalized_prefetch_empty_cache.entry"
	: >"$calls_log"

	output=$(
		(
			CALLS_LOG="$calls_log"
			CACHE_PATH="$cache_path"
			zxfer_property_cache_dataset_path() {
				g_zxfer_property_cache_path=$CACHE_PATH
				printf '%s\n' "$g_zxfer_property_cache_path"
			}
			zxfer_maybe_prefetch_recursive_normalized_properties() {
				: >"$CACHE_PATH"
				return 0
			}
			zxfer_run_zfs_cmd_for_spec() {
				printf 'call\n' >>"$CALLS_LOG"
				printf 'compression\tlz4\tlocal\n'
			}
			zxfer_load_normalized_dataset_properties "tank/src" "/sbin/zfs" source || exit $?
			printf 'props=%s\ncache_hit=%s\ncalls=%s\n' \
				"$g_zxfer_normalized_dataset_properties" \
				"${g_zxfer_normalized_dataset_properties_cache_hit:-0}" \
				"$(awk 'END {print NR + 0}' "$CALLS_LOG")"
		)
	)

	assertEquals "Empty prefetched normalized-property cache entries should be treated as cache misses and retried live." \
		"props=compression=lz4=local
cache_hit=0
calls=2" "$output"
}

test_load_normalized_dataset_properties_disables_cache_when_live_store_fails() {
	cache_dir="$TEST_TMPDIR/normalized_live_store_failure.cache"
	mkdir -p "$cache_dir"

	output=$(
		(
			CACHE_DIR="$cache_dir"
			g_zxfer_property_cache_dir=$CACHE_DIR
			zxfer_property_cache_store() {
				return 1
			}
			zxfer_run_zfs_cmd_for_spec() {
				printf 'compression\tlz4\tlocal\n'
			}
			zxfer_load_normalized_dataset_properties "tank/src" "/sbin/zfs" source || exit $?
			printf 'props=%s\ncache_hit=%s\nunavailable=%s\ndir=%s\n' \
				"$g_zxfer_normalized_dataset_properties" \
				"${g_zxfer_normalized_dataset_properties_cache_hit:-0}" \
				"${g_zxfer_property_cache_unavailable:-0}" \
				"${g_zxfer_property_cache_dir:-}"
		)
	)

	assertEquals "Live normalized-property reads should still succeed when cache publication fails." \
		"props=compression=lz4=local
cache_hit=0
unavailable=1
dir=" "$output"
	assertFalse "Live normalized-property cache store failures should remove the unhealthy per-iteration cache directory." \
		"[ -d \"$cache_dir\" ]"
}

test_zxfer_property_cache_dataset_path_encodes_dataset_name_safely() {
	l_dataset="../unsafe path:/child"
	l_expected_key=$(printf '%s' "$l_dataset" | LC_ALL=C od -An -tx1 | tr -d ' \n')

	zxfer_property_cache_dataset_path normalized destination "$l_dataset" >/dev/null

	assertEquals "Dataset cache paths should encode dataset names instead of treating them as filesystem structure." \
		"${g_zxfer_property_cache_dir}/normalized/destination/${l_expected_key}" "$g_zxfer_property_cache_path"
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
	l_normalized_cache_path=$g_zxfer_property_cache_path
	zxfer_property_cache_property_path required destination "$l_dataset" "casesensitivity" >/dev/null
	l_required_cache_path=$g_zxfer_property_cache_path
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
	g_zxfer_property_cache_read_result="stale-cache-read"

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
	assertEquals "Resetting property caches should clear cache-file read scratch state." \
		"" "${g_zxfer_property_cache_read_result:-}"
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
	l_normalized_cache_path=$g_zxfer_property_cache_path
	zxfer_property_cache_property_path required source "$l_dataset" "casesensitivity" >/dev/null
	l_required_cache_path=$g_zxfer_property_cache_path
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
	l_source_normalized_cache_path=$g_zxfer_property_cache_path
	zxfer_property_cache_property_path required source "$l_dataset" "casesensitivity" >/dev/null
	l_source_required_cache_path=$g_zxfer_property_cache_path
	zxfer_property_cache_dataset_path normalized destination "$l_dataset" >/dev/null
	l_destination_normalized_cache_path=$g_zxfer_property_cache_path
	zxfer_property_cache_property_path required destination "$l_dataset" "casesensitivity" >/dev/null
	l_destination_required_cache_path=$g_zxfer_property_cache_path
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
	g_initial_source="tank/src"
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
	g_initial_source="tank/src"
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
	g_initial_source="tank/src"
	g_recursive_source_dataset_list="tank/src
tank/src/child"
	g_LZFS="/sbin/zfs"
	zxfer_refresh_property_tree_prefetch_context

	zxfer_run_zfs_cmd_for_spec() {
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

	zxfer_get_normalized_dataset_properties "tank/src/child" "/sbin/zfs" source >"$first_out"
	zxfer_get_normalized_dataset_properties "tank/src" "/sbin/zfs" source >"$second_out"

	unset -f zxfer_run_zfs_cmd_for_spec

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

	zxfer_run_zfs_cmd_for_spec() {
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

	zxfer_get_normalized_dataset_properties "backup/dst/child" "/sbin/zfs" destination >"$first_out"
	zxfer_get_normalized_dataset_properties "backup/dst" "/sbin/zfs" destination >"$second_out"

	unset -f zxfer_run_zfs_cmd_for_spec

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

	zxfer_run_zfs_cmd_for_spec() {
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
	zxfer_exists_destination() {
		printf '1\n'
	}

	zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
		"checksum=sha256=inherited,atime=off=inherited" \
		"" \
		"checksum=sha256,atime=off" \
		"$g_test_base_readonly_properties" >"$outfile"

	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f zxfer_exists_destination

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
	g_initial_source=""
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

	zxfer_run_zfs_cmd_for_spec() {
		printf '%s\n' "ssh timeout"
		return 1
	}
	set +e
	zxfer_prefetch_recursive_normalized_properties source >/dev/null 2>&1
	read_failure_status=$?
	unset -f zxfer_run_zfs_cmd_for_spec

	g_zxfer_source_property_tree_prefetch_state=0
	zxfer_run_zfs_cmd_for_spec() {
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
	unset -f zxfer_run_zfs_cmd_for_spec
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

	zxfer_run_zfs_cmd_for_spec() {
		printf '%s\n' "ssh timeout"
		return 1
	}
	set +e
	zxfer_prefetch_recursive_normalized_properties destination >/dev/null 2>&1
	read_failure_status=$?
	unset -f zxfer_run_zfs_cmd_for_spec

	g_zxfer_destination_property_tree_prefetch_state=0
	zxfer_run_zfs_cmd_for_spec() {
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
	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f zxfer_group_recursive_property_tree_by_dataset

	assertEquals "Recursive destination property-tree prefetch should fail closed when the recursive zfs get probe fails." \
		"1" "$read_failure_status"
	assertEquals "Failed recursive destination zfs get probes should disable destination-side property-tree prefetch for the rest of the iteration." \
		"2" "${g_zxfer_destination_property_tree_prefetch_state:-0}"
	assertEquals "Destination grouping failures should also fail closed when building the prefetched property tree." \
		"1" "$group_failure_status"
}

test_zxfer_prefetch_recursive_normalized_properties_rethrows_tempfile_allocation_failures() {
	err_log="$TEST_TMPDIR/prefetch_tempfile_failure.err"
	first_stage_file="$TEST_TMPDIR/prefetch-stage-1.tmp"
	second_stage_file="$TEST_TMPDIR/prefetch-stage-2.tmp"
	temp_call_count=0
	had_errexit=0
	case $- in
	*e*) had_errexit=1 ;;
	esac

	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_source_property_tree_prefetch_state=0
	g_recursive_source_dataset_list="tank/src"

	zxfer_get_temp_file() {
		temp_call_count=$((temp_call_count + 1))
		case "$temp_call_count" in
		1)
			g_zxfer_temp_file_result=$first_stage_file
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		2)
			g_zxfer_temp_file_result=$second_stage_file
			: >"$g_zxfer_temp_file_result"
			return 0
			;;
		esac
		printf '%s\n' "Error creating temporary file." >&2
		return 1
	}

	set +e
	zxfer_prefetch_recursive_normalized_properties source >"$err_log" 2>&1
	prefetch_status=$?

	unset -f zxfer_get_temp_file
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_property_reconcile.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertEquals "Prefetch temp-file allocation failures should fail closed immediately." \
		"1" "$prefetch_status"
	assertEquals "Prefetch temp-file allocation failures should surface the original temp-file diagnostic." \
		"Error creating temporary file." "$(cat "$err_log")"
	assertEquals "Prefetch temp-file allocation failures should disable source-side prefetch for the rest of the iteration." \
		"2" "${g_zxfer_source_property_tree_prefetch_state:-0}"
	assertFalse "Prefetch temp-file allocation failures should clean up any already-allocated staging files." \
		"[ -e \"$first_stage_file\" ]"
	assertFalse "Prefetch temp-file allocation failures should clean up later allocated staging files too." \
		"[ -e \"$second_stage_file\" ]"

	if [ "$had_errexit" -eq 1 ]; then
		set -e
	fi
}

test_zxfer_prefetch_recursive_normalized_properties_rethrows_grouped_merge_failures() {
	err_log="$TEST_TMPDIR/prefetch_group_merge_failure.err"
	fake_awk="$TEST_TMPDIR/prefetch_group_merge_awk.sh"
	cache_path=""
	old_cmd_awk=${g_cmd_awk-}
	had_errexit=0
	case $- in
	*e*) had_errexit=1 ;;
	esac

	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_source_property_tree_prefetch_state=0
	g_recursive_source_dataset_list="tank/src"

	cat >"$fake_awk" <<'EOF'
#!/bin/sh
if [ "${1:-}" = "-F" ]; then
	printf '%s\n' "group merge failed" >&2
	exit 17
fi
cat
EOF
	chmod 700 "$fake_awk"
	g_cmd_awk=$fake_awk

	zxfer_run_zfs_cmd_for_spec() {
		case "$4" in
		-Hpo | -Ho)
			printf '%s\t%s\t%s\t%s\n' "tank/src" "compression" "lz4" "local"
			return 0
			;;
		esac
		return 1
	}
	zxfer_group_recursive_property_tree_by_dataset() {
		printf '%s\t%s\n' "tank/src" "compression=lz4=local"
	}

	set +e
	zxfer_prefetch_recursive_normalized_properties source >"$err_log" 2>&1
	prefetch_status=$?
	zxfer_property_cache_dataset_path normalized source "tank/src" >/dev/null
	cache_path=$g_zxfer_property_cache_path

	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f zxfer_group_recursive_property_tree_by_dataset
	g_cmd_awk=$old_cmd_awk
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_property_reconcile.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertEquals "Grouped machine/human merge failures should preserve the merge command status." \
		"17" "$prefetch_status"
	assertEquals "Grouped machine/human merge failures should surface the merge stderr." \
		"group merge failed" "$(cat "$err_log")"
	assertEquals "Grouped machine/human merge failures should disable source-side prefetch for the rest of the iteration." \
		"2" "${g_zxfer_source_property_tree_prefetch_state:-0}"
	assertFalse "Grouped machine/human merge failures should not populate prefetched cache entries." \
		"[ -e \"$cache_path\" ]"

	if [ "$had_errexit" -eq 1 ]; then
		set -e
	fi
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

test_zxfer_property_cache_path_helpers_fail_cleanly_when_key_encoding_fails() {
	output_file="$TEST_TMPDIR/property_cache_path_failures.out"

	zxfer_property_cache_encode_key() {
		case "${1:-}" in
		tank/src | compression)
			return 1
			;;
		esac
		printf '%s\n' "encoded"
	}
	zxfer_ensure_property_cache_dir() {
		g_zxfer_property_cache_dir="$TEST_TMPDIR/property-cache"
		mkdir -p "$g_zxfer_property_cache_dir/normalized/source"
	}

	set +e
	zxfer_property_cache_dataset_path normalized source "tank/src" >/dev/null 2>&1
	dataset_status=$?
	zxfer_property_cache_property_path required source "tank/src" "compression" >/dev/null 2>&1
	property_status=$?
	set -e

	unset -f zxfer_property_cache_encode_key
	unset -f zxfer_ensure_property_cache_dir
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_property_reconcile.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	{
		printf 'dataset=%s\n' "$dataset_status"
		printf 'property=%s\n' "$property_status"
	} >"$output_file"

	assertEquals "Property-cache path helpers should fail cleanly when dataset or property key encoding fails." \
		"dataset=1
property=1" "$(cat "$output_file")"
}

test_zxfer_property_cache_store_fails_when_parent_directory_cannot_be_created() {
	set +e
	output=$(
		(
			mkdir() {
				return 1
			}
			zxfer_property_cache_store \
				"$TEST_TMPDIR/missing/normalized/cache-entry" "value"
		)
	)
	status=$?

	assertEquals "Property-cache stores should fail cleanly when their parent directory cannot be created." \
		1 "$status"
	assertEquals "Failed property-cache stores should not emit a payload." "" "$output"
}

test_zxfer_invalidate_dataset_property_cache_ignores_key_encoding_failures() {
	g_zxfer_property_cache_dir="$TEST_TMPDIR/cache-invalidate"
	mkdir -p "$g_zxfer_property_cache_dir/normalized/source" \
		"$g_zxfer_property_cache_dir/required/source"
	: >"$g_zxfer_property_cache_dir/normalized/source/stale"
	: >"$g_zxfer_property_cache_dir/required/source/stale"

	zxfer_property_cache_encode_key() {
		return 1
	}
	zxfer_invalidate_dataset_property_cache source "tank/src"
	unset -f zxfer_property_cache_encode_key
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_property_reconcile.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertTrue "Dataset-cache invalidation should return quietly when key encoding fails and leave unrelated cache files untouched." \
		"[ -f '$g_zxfer_property_cache_dir/normalized/source/stale' ]"
	assertTrue "Dataset-cache invalidation should not remove required-property cache entries when key encoding fails." \
		"[ -f '$g_zxfer_property_cache_dir/required/source/stale' ]"
}

test_zxfer_prefetch_recursive_normalized_properties_disables_human_tree_read_failures() {
	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_source_property_tree_prefetch_state=0
	g_recursive_source_dataset_list="tank/src"
	output_file="$TEST_TMPDIR/prefetch_human_tree_failure.out"

	zxfer_run_zfs_cmd_for_spec() {
		case "$4" in
		-Hpo)
			printf '%s\t%s\t%s\t%s\n' "tank/src" "compression" "lz4" "local"
			return 0
			;;
		-Ho)
			printf '%s\n' "human read failed"
			return 1
			;;
		esac
		return 1
	}

	set +e
	zxfer_prefetch_recursive_normalized_properties source >/dev/null
	prefetch_status=$?
	set -e

	unset -f zxfer_run_zfs_cmd_for_spec
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_property_reconcile.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	{
		printf 'status=%s\n' "$prefetch_status"
		printf 'state=%s\n' "${g_zxfer_source_property_tree_prefetch_state:-0}"
	} >"$output_file"

	assertEquals "Human-tree read failures should disable recursive source property prefetch for the rest of the iteration." \
		"status=1
state=2" "$(cat "$output_file")"
}

test_zxfer_prefetch_recursive_normalized_properties_disables_cache_apply_failures() {
	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_source_property_tree_prefetch_state=0
	g_recursive_source_dataset_list="tank/src"
	output_file="$TEST_TMPDIR/prefetch_cache_apply_failure.out"

	zxfer_run_zfs_cmd_for_spec() {
		case "$4" in
		-Hpo | -Ho)
			printf '%s\t%s\t%s\t%s\n' "tank/src" "compression" "lz4" "local"
			return 0
			;;
		esac
		return 1
	}
	zxfer_property_cache_store() {
		return 1
	}

	set +e
	zxfer_prefetch_recursive_normalized_properties source >/dev/null 2>&1
	prefetch_status=$?
	set -e

	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f zxfer_property_cache_store
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_property_reconcile.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	{
		printf 'status=%s\n' "$prefetch_status"
		printf 'state=%s\n' "${g_zxfer_source_property_tree_prefetch_state:-0}"
	} >"$output_file"

	assertEquals "Recursive source property prefetch should fail closed when grouped cache entries cannot be published." \
		"status=1
state=2" "$(cat "$output_file")"
}

test_zxfer_maybe_prefetch_recursive_normalized_properties_fails_when_prefetch_or_cache_path_creation_fails() {
	output_file="$TEST_TMPDIR/maybe_prefetch_failure.out"

	g_zxfer_destination_property_tree_prefetch_root="backup/dst"
	g_zxfer_destination_property_tree_prefetch_zfs_cmd="/dest/zfs"
	g_recursive_dest_list="backup/dst"

	zxfer_prefetch_recursive_normalized_properties() {
		return 1
	}
	set +e
	zxfer_maybe_prefetch_recursive_normalized_properties "backup/dst" "/dest/zfs" destination >/dev/null 2>&1
	prefetch_status=$?
	unset -f zxfer_prefetch_recursive_normalized_properties

	zxfer_prefetch_recursive_normalized_properties() {
		return 0
	}
	zxfer_property_cache_dataset_path() {
		return 1
	}
	zxfer_maybe_prefetch_recursive_normalized_properties "backup/dst" "/dest/zfs" destination >/dev/null 2>&1
	cache_path_status=$?
	set -e

	unset -f zxfer_prefetch_recursive_normalized_properties
	unset -f zxfer_property_cache_dataset_path
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_property_reconcile.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	{
		printf 'prefetch=%s\n' "$prefetch_status"
		printf 'cache_path=%s\n' "$cache_path_status"
	} >"$output_file"

	assertEquals "Destination prefetch lookups should fail cleanly when the prefetch pass fails or the cache path cannot be derived." \
		"prefetch=1
cache_path=1" "$(cat "$output_file")"
}

test_zxfer_get_required_property_probe_defaults_to_local_zfs_command_when_unspecified() {
	log="$TEST_TMPDIR/required_probe_default_zfs.log"
	: >"$log"
	g_LZFS="/default/zfs"

	zxfer_run_zfs_cmd_for_spec() {
		printf '%s\n' "$1" >>"$log"
		printf 'casesensitivity\tsensitive\tlocal\n'
	}

	zxfer_get_required_property_probe "tank/src" "casesensitivity" "" source
	result=$g_zxfer_required_property_probe_result

	unset -f zxfer_run_zfs_cmd_for_spec
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_property_reconcile.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertEquals "Required-property probes should fall back to the local source zfs helper when no command is provided." \
		"/default/zfs" "$(cat "$log")"
	assertEquals "Default-zfs required-property probes should preserve the parsed property payload." \
		"casesensitivity=sensitive=local" "$result"
}

test_zxfer_get_required_property_probe_reports_serializer_failures_without_caching() {
	err_log="$TEST_TMPDIR/required_probe_serializer_failure.err"
	calls_log="$TEST_TMPDIR/required_probe_serializer_failure.calls"
	: >"$calls_log"

	zxfer_run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		printf 'casesensitivity\tsensitive\tlocal\n'
	}

	zxfer_serialize_property_records_from_stdin() {
		printf '%s\n' "required serializer failed" >&2
		return 25
	}

	set +e
	zxfer_get_required_property_probe "tank/src" "casesensitivity" "/sbin/zfs" source >"$err_log" 2>&1
	status=$?
	zxfer_property_cache_property_path required source "tank/src" "casesensitivity" >/dev/null
	cache_path=$g_zxfer_property_cache_path

	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f zxfer_serialize_property_records_from_stdin
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_property_reconcile.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertNotEquals "Required-property serializer failures should return a non-zero status." \
		"0" "$status"
	assertEquals "Required-property serializer failures should surface the serializer stderr." \
		"required serializer failed" "$(cat "$err_log")"
	assertEquals "Required-property serializer failures should leave the parsed result empty." \
		"" "${g_zxfer_required_property_probe_result:-}"
	assertNotContains "Required-property serializer failures should not be downgraded into generic parse errors." \
		"$(cat "$err_log")" "Failed to parse required creation-time property"
	assertEquals "Required-property serializer failures should not execute extra zfs probes." \
		"1" "$(awk 'END {print NR + 0}' "$calls_log")"
	assertFalse "Required-property serializer failures should not populate the required-property cache." \
		"[ -e \"$cache_path\" ]"
}

test_zxfer_get_required_property_probe_reports_serializer_readback_failures_without_caching() {
	err_log="$TEST_TMPDIR/required_probe_serializer_readback_failure.err"
	calls_log="$TEST_TMPDIR/required_probe_serializer_readback_failure.calls"
	staged_output_file="$TEST_TMPDIR/required_probe_serializer_readback_failure.stage"
	: >"$calls_log"

	zxfer_get_temp_file() {
		g_zxfer_temp_file_result="$staged_output_file"
		: >"$g_zxfer_temp_file_result"
	}

	zxfer_run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		printf 'casesensitivity\tsensitive\tlocal\n'
	}

	cat() {
		if [ "$1" = "$staged_output_file" ]; then
			printf '%s\n' "required serializer readback failed" >&2
			printf '%s\n' "casesensitivity=sensitive=local"
			return 27
		fi
		command cat "$@"
	}

	set +e
	zxfer_get_required_property_probe "tank/src" "casesensitivity" "/sbin/zfs" source >"$err_log" 2>&1
	status=$?
	zxfer_property_cache_property_path required source "tank/src" "casesensitivity" >/dev/null
	cache_path=$g_zxfer_property_cache_path

	unset -f zxfer_get_temp_file
	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f cat
	ZXFER_SOURCE_MODULES_ROOT=$ZXFER_ROOT ZXFER_SOURCE_MODULES_THROUGH=zxfer_property_reconcile.sh . "$ZXFER_ROOT/src/zxfer_modules.sh"

	assertEquals "Required-property serializer readback failures should return the staged readback status." \
		"27" "$status"
	assertEquals "Required-property serializer readback failures should surface the staged readback diagnostic." \
		"required serializer readback failed" "$(cat "$err_log")"
	assertEquals "Required-property serializer readback failures should leave the parsed result empty." \
		"" "${g_zxfer_required_property_probe_result:-}"
	assertNotContains "Required-property serializer readback failures should not be downgraded into generic parse errors." \
		"$(cat "$err_log")" "Failed to parse required creation-time property"
	assertEquals "Required-property serializer readback failures should not execute extra zfs probes." \
		"1" "$(awk 'END {print NR + 0}' "$calls_log")"
	assertFalse "Required-property serializer readback failures should not populate the required-property cache." \
		"[ -e \"$cache_path\" ]"
}

test_zxfer_get_required_property_probe_falls_back_to_live_probe_when_cache_read_fails() {
	calls_log="$TEST_TMPDIR/required_probe_cache_read_failure.calls"
	cache_path="$TEST_TMPDIR/required_probe_cache_read_failure.entry"
	: >"$calls_log"
	printf '%s\n' "__ZXFER_REQUIRED_PROPERTY_UNSUPPORTED__" >"$cache_path"

	output=$(
		(
			CALLS_LOG="$calls_log"
			CACHE_PATH="$cache_path"
			zxfer_property_cache_property_path() {
				g_zxfer_property_cache_path=$CACHE_PATH
				printf '%s\n' "$g_zxfer_property_cache_path"
			}
			zxfer_read_property_cache_file() {
				return 1
			}
			zxfer_run_zfs_cmd_for_spec() {
				printf 'call\n' >>"$CALLS_LOG"
				printf 'casesensitivity\tsensitive\tlocal\n'
			}
			zxfer_get_required_property_probe "tank/src" "casesensitivity" "/sbin/zfs" source || exit $?
			printf 'result=%s\ncalls=%s\n' \
				"$g_zxfer_required_property_probe_result" \
				"$(awk 'END {print NR + 0}' "$CALLS_LOG")"
		)
	)

	assertEquals "Unreadable required-property cache entries should be treated as cache misses and retried live." \
		"result=casesensitivity=sensitive=local
calls=1" "$output"
}

test_zxfer_get_required_property_probe_falls_back_to_live_probe_when_cache_read_returns_partial_failure() {
	calls_log="$TEST_TMPDIR/required_probe_cache_partial_read_failure.calls"
	cache_path="$TEST_TMPDIR/required_probe_cache_partial_read_failure.entry"
	: >"$calls_log"
	printf '%s\n' "__ZXFER_REQUIRED_PROPERTY_UNSUPPORTED__" >"$cache_path"

	output=$(
		(
			CALLS_LOG="$calls_log"
			CACHE_PATH="$cache_path"
			zxfer_property_cache_property_path() {
				g_zxfer_property_cache_path=$CACHE_PATH
				printf '%s\n' "$g_zxfer_property_cache_path"
			}
			zxfer_read_property_cache_file() {
				g_zxfer_property_cache_read_result="__ZXFER_REQUIRED_PROPERTY_UNSUPPORTED__"
				return 9
			}
			zxfer_run_zfs_cmd_for_spec() {
				printf 'call\n' >>"$CALLS_LOG"
				printf 'casesensitivity\tsensitive\tlocal\n'
			}
			zxfer_get_required_property_probe "tank/src" "casesensitivity" "/sbin/zfs" source || exit $?
			printf 'result=%s\ncalls=%s\n' \
				"$g_zxfer_required_property_probe_result" \
				"$(awk 'END {print NR + 0}' "$CALLS_LOG")"
		)
	)

	assertEquals "Partial required-property cache reads that fail nonzero should still be treated as cache misses and retried live." \
		"result=casesensitivity=sensitive=local
calls=1" "$output"
}

test_zxfer_get_required_property_probe_falls_back_to_live_probe_when_cache_entry_is_empty() {
	calls_log="$TEST_TMPDIR/required_probe_empty_cache.calls"
	cache_path="$TEST_TMPDIR/required_probe_empty_cache.entry"
	: >"$calls_log"
	: >"$cache_path"

	output=$(
		(
			CALLS_LOG="$calls_log"
			CACHE_PATH="$cache_path"
			zxfer_property_cache_property_path() {
				g_zxfer_property_cache_path=$CACHE_PATH
				printf '%s\n' "$g_zxfer_property_cache_path"
			}
			zxfer_run_zfs_cmd_for_spec() {
				printf 'call\n' >>"$CALLS_LOG"
				printf 'casesensitivity\tsensitive\tlocal\n'
			}
			zxfer_get_required_property_probe "tank/src" "casesensitivity" "/sbin/zfs" source || exit $?
			printf 'result=%s\ncalls=%s\n' \
				"$g_zxfer_required_property_probe_result" \
				"$(awk 'END {print NR + 0}' "$CALLS_LOG")"
		)
	)

	assertEquals "Empty required-property cache entries should be treated as cache misses and retried live." \
		"result=casesensitivity=sensitive=local
calls=1" "$output"
}

test_zxfer_get_required_property_probe_disables_cache_when_live_store_fails() {
	cache_dir="$TEST_TMPDIR/required_probe_live_store_failure.cache"
	mkdir -p "$cache_dir"

	output=$(
		(
			CACHE_DIR="$cache_dir"
			g_zxfer_property_cache_dir=$CACHE_DIR
			zxfer_property_cache_store() {
				return 1
			}
			zxfer_run_zfs_cmd_for_spec() {
				printf 'casesensitivity\tsensitive\tlocal\n'
			}
			zxfer_get_required_property_probe "tank/src" "casesensitivity" "/sbin/zfs" source || exit $?
			printf 'result=%s\nunavailable=%s\ndir=%s\n' \
				"$g_zxfer_required_property_probe_result" \
				"${g_zxfer_property_cache_unavailable:-0}" \
				"${g_zxfer_property_cache_dir:-}"
		)
	)

	assertEquals "Live required-property probes should still succeed when cache publication fails." \
		"result=casesensitivity=sensitive=local
unavailable=1
dir=" "$output"
	assertFalse "Live required-property cache store failures should remove the unhealthy per-iteration cache directory." \
		"[ -d \"$cache_dir\" ]"
}

test_force_readonly_off_handles_empty_and_rewrites_property() {
	assertEquals "Empty property lists should stay empty." "" "$(zxfer_force_readonly_off "")"
	assertEquals "readonly=on entries should be forced to readonly=off." \
		"readonly=off=local,compression=lz4=local" \
		"$(zxfer_force_readonly_off "readonly=on=local,compression=lz4=local")"
}

test_collect_source_props_uses_backup_restore_and_force_writable() {
	output_file="$TEST_TMPDIR/collect_source_restore.out"

	(
		zxfer_get_normalized_dataset_properties() {
			printf '%s\n' "compression=lz4=local,readonly=on=local"
		}
		g_option_e_restore_property_mode=1
		g_restored_backup_file_contents=$(zxfer_test_render_current_backup_metadata_contents \
			"tank/src,backup/dst,readonly=on=local,compression=lz4=local")
		zxfer_collect_source_props "tank/src" "backup/dst" 1 ""
		printf 'raw=%s\n' "$g_zxfer_source_pvs_raw" >"$output_file"
		printf 'effective=%s\n' "$g_zxfer_source_pvs_effective" >>"$output_file"
	)

	result=$(cat "$output_file")
	assertContains "Raw source properties should come from the live source query." \
		"$result" "raw=compression=lz4=local,readonly=on=local"
	assertContains "Restore mode should pull the backup entry and force readonly=off when requested." \
		"$result" "effective=readonly=off=local,compression=lz4=local"
}

test_collect_source_props_restore_mode_requires_exact_destination_match() {
	set +e
	output=$(
		(
			zxfer_get_normalized_dataset_properties() {
				printf '%s\n' "compression=lz4=local"
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			g_restored_backup_file_contents=$(zxfer_test_render_current_backup_metadata_contents \
				"tank/src,backup/other,compression=off=local" \
				"backup/other,tank/src,compression=off=local")
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
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
			zxfer_get_normalized_dataset_properties() {
				printf '%s\n' "compression=lz4=local"
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			g_restored_backup_file_contents=""
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
		)
	)
	status=$?

	assertEquals "Missing restored property metadata should abort with usage status." 2 "$status"
	assertContains "Missing restored property metadata should identify both the source and destination datasets." \
		"$output" "Can't find the properties for the filesystem tank/src and destination backup/dst"
}

test_collect_source_props_restore_mode_requires_backup_metadata_header() {
	set +e
	output=$(
		(
			zxfer_get_normalized_dataset_properties() {
				printf '%s\n' "compression=lz4=local"
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			g_restored_backup_file_contents="tank/src,backup/dst,compression=off=local"
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
		)
	)
	status=$?

	assertEquals "Restore-mode source collection should fail closed when restored metadata lacks the required header." \
		2 "$status"
	assertContains "Missing-header restore failures should identify that the metadata must start with the exact zxfer header." \
		"$output" "Restored properties for the filesystem tank/src and destination backup/dst do not start with the required zxfer backup metadata header"
}

test_collect_source_props_restore_mode_requires_backup_metadata_header_first() {
	set +e
	output=$(
		(
			zxfer_get_normalized_dataset_properties() {
				printf '%s\n' "compression=lz4=local"
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			g_restored_backup_file_contents=$(printf '%s\n%s\n%s\n%s\n%s\n' \
				"#legacy comment" \
				"#zxfer property backup file" \
				"#format_version:1" \
				"#version:test-version" \
				"tank/src,backup/dst,compression=off=local")
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
		)
	)
	status=$?

	assertEquals "Restore-mode source collection should fail closed when restored metadata places content before the zxfer header." \
		2 "$status"
	assertContains "Misordered-header restore failures should explain that the metadata must start with the required zxfer header." \
		"$output" "Restored properties for the filesystem tank/src and destination backup/dst do not start with the required zxfer backup metadata header"
}

test_collect_source_props_restore_mode_rejects_unknown_backup_metadata_format_version() {
	set +e
	output=$(
		(
			zxfer_get_normalized_dataset_properties() {
				printf '%s\n' "compression=lz4=local"
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			g_restored_backup_file_contents=$(printf '%s\n%s\n%s\n%s\n' \
				"#zxfer property backup file" \
				"#format_version:999" \
				"#version:test-version" \
				"tank/src,backup/dst,compression=off=local")
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
		)
	)
	status=$?

	assertEquals "Restore-mode source collection should fail closed on unsupported backup metadata schema versions." \
		2 "$status"
	assertContains "Unknown-format restore failures should identify the expected schema marker." \
		"$output" "Restored properties for the filesystem tank/src and destination backup/dst do not declare supported zxfer backup metadata format version #format_version:1"
}

test_collect_source_props_propagates_normalized_property_lookup_failures() {
	set +e
	output=$(
		(
			zxfer_get_normalized_dataset_properties() {
				printf '%s\n' "permission denied"
				return 1
			}
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
		)
	)
	status=$?

	assertEquals "Source property collection should return a failure when normalized source inspection fails." \
		"1" "$status"
	assertEquals "Source property collection should preserve the normalized-property lookup failure output." \
		"permission denied" "$output"
}

test_collect_source_props_rethrows_tempfile_allocation_failures() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_get_temp_file() {
				zxfer_throw_error "Error creating temporary file."
			}
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
		) 2>&1
	)
	status=$?

	assertEquals "Source property collection should fail closed when temp-file allocation fails." \
		"1" "$status"
	assertEquals "Source property collection should preserve the temp-file allocation failure." \
		"Error creating temporary file." "$output"
}

test_collect_source_props_rejects_ambiguous_restore_entries_for_exact_pair() {
	set +e
	output=$(
		(
			zxfer_get_normalized_dataset_properties() {
				printf '%s\n' "compression=lz4=local"
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			g_restored_backup_file_contents=$(zxfer_test_render_current_backup_metadata_contents \
				"tank/src,backup/dst,compression=lz4=local" \
				"tank/src,backup/dst,compression=off=local")
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
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
		zxfer_get_normalized_dataset_properties() {
			printf '%s\n' "compression=off=local"
		}
		g_option_e_restore_property_mode=1
		g_restored_backup_file_contents=$(zxfer_test_render_current_backup_metadata_contents \
			"tank/src/child.tail-010,backup/dst,user:note=value%2Cwith%2Ccommas=local" \
			"tank/src/child.tail-01,backup/dst,user:note=value%3Dwith%3Dequals%3Band%3Bsemicolon=local")
		zxfer_collect_source_props "tank/src/child.tail-01" "backup/dst" 0 ""
		printf '%s\n' "$g_zxfer_source_pvs_effective" >"$output_file"
	)

	assertEquals "Restore-mode source matching should select the exact awkward dataset tail and preserve the encoded serialized payload." \
		"user:note=value%3Dwith%3Dequals%3Band%3Bsemicolon=local" "$(cat "$output_file")"
}

test_validate_override_properties_returns_success_for_empty_list_in_current_shell() {
	zxfer_validate_override_properties "" "compression=lz4=local"
	status=$?

	assertEquals "Empty override lists should validate successfully." 0 "$status"
}

test_validate_override_properties_rejects_missing_source_property() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_validate_override_properties "quota=1G" "compression=lz4=local"
		)
	)
	status=$?

	assertEquals "Override validation should fail when -o references a property absent from the source set." \
		"1" "$status"
	assertContains "Override validation failures should preserve the current usage-error message." \
		"$output" "Invalid option property"
}

test_validate_override_properties_accepts_escaped_commas_in_current_shell() {
	zxfer_validate_override_properties "user:note=value\\,with\\,commas" "user:note=existing=local"
	status=$?

	assertEquals "Override validation should accept literal commas escaped as \\, inside one -o value." \
		0 "$status"
}

test_validate_override_properties_rejects_missing_assignment_separator() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_validate_override_properties "compression" "compression=lz4=local"
		)
	)
	status=$?

	assertEquals "Override validation should reject -o items that omit the assignment separator even when the property exists on the source." \
		"1" "$status"
	assertContains "Malformed override syntax should preserve the current usage-error message." \
		"$output" "Invalid option property"
}

test_derive_override_lists_preserves_override_only_mode_order() {
	output_file="$TEST_TMPDIR/derive_override_only.out"

	zxfer_derive_override_lists "" "compression=lz4,quota=1G" "0" "filesystem" >"$output_file"

	assertEquals "Override-only derivation should emit the requested override list in option order." \
		"compression=lz4=override,quota=1G=override" "$(sed -n '1p' "$output_file")"
	assertEquals "Override-only derivation should leave the creation-property list empty." \
		"" "$(sed -n '2p' "$output_file")"
}

test_derive_override_lists_prefers_first_matching_override_when_transferring_all_properties() {
	output_file="$TEST_TMPDIR/derive_override_all.out"

	zxfer_derive_override_lists \
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

	zxfer_derive_override_lists "" "user:note=value=with=equals;semi" "0" "filesystem" >"$output_file"

	assertEquals "Override derivation should preserve the full override value by escaping internal delimiters after the first equals sign." \
		"user:note=value%3Dwith%3Dequals%3Bsemi=override" "$(sed -n '1p' "$output_file")"
	assertEquals "Override-only derivation should still leave the creation-property list empty for delimiter-heavy values." \
		"" "$(sed -n '2p' "$output_file")"
}

test_derive_override_lists_escapes_literal_commas_inside_override_values() {
	output_file="$TEST_TMPDIR/derive_override_escaped_commas.out"

	zxfer_derive_override_lists "" "user:note=value\\,with\\,commas=and;semi" "0" "filesystem" >"$output_file"

	assertEquals "Override derivation should decode escaped commas before storing the internal encoded value." \
		"user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=override" "$(sed -n '1p' "$output_file")"
	assertEquals "Override-only derivation should still leave the creation-property list empty when a value contains escaped commas." \
		"" "$(sed -n '2p' "$output_file")"
}

test_derive_override_lists_preserves_literal_backslashes() {
	output_file="$TEST_TMPDIR/derive_override_backslashes.out"

	zxfer_derive_override_lists "" 'user:path=C:\\temp\\logs' "0" "filesystem" >"$output_file"

	assertEquals "Override derivation should not collapse literal backslashes that are not escaping commas." \
		'user:path=C:\\temp\\logs=override' "$(sed -n '1p' "$output_file")"
	assertEquals "Backslash-only values should still leave the creation-property list empty in override-only mode." \
		"" "$(sed -n '2p' "$output_file")"
}

test_derive_override_lists_skips_volume_only_properties_for_filesystems() {
	output_file="$TEST_TMPDIR/derive_override_no_volume_only.out"

	zxfer_derive_override_lists \
		"volblocksize=16K=local,volthreading=on=local,compression=lz4=local" \
		"" \
		"1" \
		"filesystem" >"$output_file"

	assertEquals "Filesystem property transfer should not carry zvol-only properties into the override list." \
		"compression=lz4=local" "$(sed -n '1p' "$output_file")"
	assertEquals "Filesystem property transfer should still keep legitimate local filesystem creation properties when zvol-only properties are filtered out." \
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
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_validate_override_properties "compression=lz4" "compression=lz4=local"
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
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_derive_override_lists "compression=lz4=local" "compression=gzip-9" "1" "filesystem"
		)
	)
	status=$?

	assertEquals "Override-list derivation should fail closed when its awk helper cannot execute." \
		"1" "$status"
	assertContains "Override-list derivation awk failures should surface the helper failure message." \
		"$output" "Failed to derive override property lists."
}

test_derive_override_lists_rejects_missing_assignment_separator() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_derive_override_lists "compression=lz4=local" "compression" "1" "filesystem"
		)
	)
	status=$?

	assertEquals "Override derivation should reject malformed -o items instead of silently skipping them." \
		"1" "$status"
	assertContains "Malformed derive-time overrides should preserve the current usage-error message." \
		"$output" "Invalid option property"
}

test_sanitize_property_list_returns_empty_for_empty_input() {
	assertEquals "Empty property lists should remain empty after sanitization." "" \
		"$(zxfer_sanitize_property_list "" "$g_test_base_readonly_properties" "$g_option_I_ignore_properties")"
}

test_strip_unsupported_properties_returns_input_when_no_unsupported_properties() {
	assertEquals "Unsupported-property stripping should no-op when no unsupported list is present." \
		"compression=lz4=local" "$(zxfer_strip_unsupported_properties "compression=lz4=local" "")"
}

test_strip_unsupported_properties_honors_explicit_unsupported_list_argument() {
	g_unsupported_properties="compression"

	assertEquals "Unsupported-property stripping should honor the explicit unsupported list it is passed instead of the run-global scratch state." \
		"compression=lz4=local" "$(zxfer_strip_unsupported_properties "compression=lz4=local,quota=1G=local" "quota")"
}

test_remove_unsupported_properties_honors_explicit_unsupported_list_argument() {
	g_unsupported_properties="compression"

	zxfer_remove_unsupported_properties "compression=lz4=local,quota=1G=local" "quota"

	assertEquals "Unsupported-property filtering should honor the explicit unsupported list it is passed instead of the run-global scratch state." \
		"compression=lz4=local" "$g_zxfer_only_supported_properties"
}

test_remove_unsupported_properties_rethrows_tempfile_allocation_failures() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_get_temp_file() {
				zxfer_throw_error "Error creating temporary file."
			}
			zxfer_remove_unsupported_properties "compression=lz4=local,quota=1G=local" "quota"
		) 2>&1
	)
	status=$?

	assertEquals "Unsupported-property filtering should fail closed when temp-file allocation fails." \
		"1" "$status"
	assertEquals "Unsupported-property filtering should preserve the temp-file allocation failure." \
		"Error creating temporary file." "$output"
}

test_strip_unsupported_properties_keeps_stdout_clean_when_verbose() {
	stdout_log="$TEST_TMPDIR/unsupported_stdout.log"
	stderr_log="$TEST_TMPDIR/unsupported_stderr.log"
	g_unsupported_properties="compression"
	g_option_v_verbose=1

	zxfer_strip_unsupported_properties "compression=lz4=local,quota=1G=local" "$g_unsupported_properties" >"$stdout_log" 2>"$stderr_log"

	assertEquals "Unsupported-property filtering should return only supported properties on stdout." \
		"quota=1G=local" "$(cat "$stdout_log")"
	assertContains "Verbose unsupported-property notices should go to stderr." \
		"$(cat "$stderr_log")" "Destination does not support property compression=lz4"
}

test_strip_unsupported_properties_decodes_verbose_delimiter_heavy_values() {
	stdout_log="$TEST_TMPDIR/unsupported_encoded_stdout.log"
	stderr_log="$TEST_TMPDIR/unsupported_encoded_stderr.log"
	g_unsupported_properties="user:note"
	g_option_v_verbose=1

	zxfer_strip_unsupported_properties "user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=local" "$g_unsupported_properties" >"$stdout_log" 2>"$stderr_log"

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
			g_unsupported_properties="compression"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$TEST_TMPDIR/unsupported_awk_failure.tmp"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_strip_unsupported_properties "compression=lz4=local" "$g_unsupported_properties"
		) 2>&1
	)
	status=$?

	assertEquals "Unsupported-property filtering should fail closed when its awk helper cannot execute." \
		"1" "$status"
	assertContains "Unsupported-property filtering awk failures should surface the helper failure message." \
		"$output" "Failed to filter unsupported destination properties."
}

test_remove_unsupported_properties_preserves_readback_failures_without_publishing_results() {
	set +e
	output=$(
		(
			l_tmp_path="$TEST_TMPDIR/unsupported_readback_failure.tmp"
			g_zxfer_only_supported_properties="stale"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$l_tmp_path
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_read_property_reconcile_stage_file() {
				return 1
			}
			zxfer_remove_unsupported_properties "compression=lz4=local,quota=1G=local" "quota" || {
				printf 'supported=<%s>\n' "$g_zxfer_only_supported_properties"
				if [ -e "$l_tmp_path" ]; then
					printf 'tmp_exists=yes\n'
				else
					printf 'tmp_exists=no\n'
				fi
				exit 1
			}
		)
	)
	status=$?

	assertEquals "Unsupported-property filtering should fail closed when staged readback fails." \
		"1" "$status"
	assertContains "Unsupported-property staged readback failures should not publish a partial supported-property list." \
		"$output" "supported=<>"
	assertContains "Unsupported-property staged readback failures should still clean the staged temp file." \
		"$output" "tmp_exists=no"
}

test_calculate_unsupported_properties_uses_direct_destination_property_probes() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"
	probe_log="$TEST_TMPDIR/unsupported_probe.log"
	: >"$probe_log"

	(
		PROBE_LOG="$probe_log"
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$PROBE_LOG"
			case "$*" in
			"get -Hpo value type backup/dst")
				printf 'filesystem\n'
				;;
			"get -Hpo property,value,source compression backup/dst")
				printf 'compression\tlz4\tlocal\n'
				;;
			"get -Hpo property,value,source user:note backup/dst")
				printf 'user:note\t-\t-\n'
				;;
			"get -Hpo property,value,source recordsize backup/dst")
				printf '%s\n' "invalid property"
				return 1
				;;
			esac
		}
		zxfer_run_source_zfs_cmd() {
			case "$*" in
			"get -Hpo value type tank/src")
				printf 'filesystem\n'
				;;
			"get -Hpo property all tank/src")
				printf 'compression\nuser:note\nrecordsize\n'
				;;
			esac
		}
		zxfer_calculate_unsupported_properties
		printf '%s\n' "$g_unsupported_properties"
	) >"$TEST_TMPDIR/unsupported_props.out"

	assertEquals "Direct destination property probes should only mark properties unsupported when the destination rejects that specific property name." \
		"recordsize" "$(cat "$TEST_TMPDIR/unsupported_props.out")"
	assertContains "Direct destination property probes should validate user properties even when they are merely absent on the destination root." \
		"$(cat "$probe_log")" "get -Hpo property,value,source user:note backup/dst"
}

test_calculate_unsupported_properties_falls_back_to_destination_pool_when_root_is_missing() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"
	probe_log="$TEST_TMPDIR/unsupported_pool_probe.log"
	: >"$probe_log"

	(
		PROBE_LOG="$probe_log"
		zxfer_exists_destination() {
			printf '0\n'
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$PROBE_LOG"
			case "$*" in
			"get -Hpo value type backup")
				printf 'filesystem\n'
				;;
			"get -Hpo property,value,source compression backup")
				printf 'compression\tlz4\tlocal\n'
				;;
			esac
		}
		zxfer_run_source_zfs_cmd() {
			case "$*" in
			"get -Hpo value type tank/src")
				printf 'filesystem\n'
				;;
			"get -Hpo property all tank/src")
				printf 'compression\n'
				;;
			esac
		}
		zxfer_calculate_unsupported_properties
		printf '%s\n' "$g_unsupported_properties"
	) >"$TEST_TMPDIR/unsupported_pool_props.out"

	assertEquals "Missing destination roots should fall back to the destination pool for unsupported-property probes." \
		"" "$(cat "$TEST_TMPDIR/unsupported_pool_props.out")"
	assertContains "Missing destination roots should probe the destination pool instead of the absent dataset path." \
		"$(cat "$probe_log")" "get -Hpo property,value,source compression backup"
}

test_calculate_unsupported_properties_treats_blank_pool_fallback_probe_failures_as_unsupported_when_destination_is_still_queryable() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"
	probe_log="$TEST_TMPDIR/unsupported_blank_pool_probe.log"
	: >"$probe_log"

	(
		PROBE_LOG="$probe_log"
		zxfer_exists_destination() {
			printf '0\n'
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$PROBE_LOG"
			if [ "$*" = "get -Hpo value type backup" ]; then
				printf 'filesystem\n'
				return 0
			fi
			if [ "$*" = "get -Hpo property,value,source compression backup" ]; then
				return 1
			fi
			if [ "$*" = "get -Hpo property all backup" ]; then
				printf 'available\n'
			fi
		}
		zxfer_run_source_zfs_cmd() {
			case "$*" in
			"get -Hpo value type tank/src")
				printf 'filesystem\n'
				;;
			"get -Hpo property all tank/src")
				printf 'compression\n'
				;;
			esac
		}
		zxfer_calculate_unsupported_properties
		printf '%s\n' "$g_unsupported_properties"
	) >"$TEST_TMPDIR/unsupported_blank_pool_props.out"

	assertEquals "Blank pool-root property probe failures should still classify the property as unsupported when the destination dataset remains queryable." \
		"compression" "$(cat "$TEST_TMPDIR/unsupported_blank_pool_props.out")"
	assertContains "Blank pool-root probe failures should confirm the destination dataset is otherwise queryable before downgrading to unsupported." \
		"$(cat "$probe_log")" "get -Hpo property all backup"
}

test_get_unsupported_property_probe_dataset_reports_missing_destination_context() {
	set +e
	output=$(
		(
			g_destination=""
			zxfer_get_unsupported_property_probe_dataset ""
		)
	)
	status=$?

	assertEquals "Unsupported-property probe dataset lookup should fail when neither the requested destination nor g_destination is available." \
		"1" "$status"
	assertContains "Missing unsupported-property probe datasets should surface the dedicated helper message." \
		"$output" "Failed to determine the destination property-support probe dataset."
}

test_get_unsupported_property_probe_dataset_reports_destination_lookup_failure() {
	g_destination="backup/dst"
	g_initial_source_had_trailing_slash=1

	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '%s\n' "lookup failure"
				return 1
			}
			zxfer_get_unsupported_property_probe_dataset "$g_destination"
		)
	)
	status=$?

	assertEquals "Unsupported-property probe dataset lookup should fail closed when destination existence checks fail." \
		"1" "$status"
	assertContains "Unsupported-property probe dataset lookup should preserve the destination existence failure details." \
		"$output" "Failed to determine whether destination dataset [backup/dst] exists: lookup failure"
}

test_get_unsupported_property_probe_dataset_type_reports_lookup_failure() {
	set +e
	output=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "type lookup failure"
				return 1
			}
			zxfer_get_unsupported_property_probe_dataset_type "backup/dst"
		)
	)
	status=$?

	assertEquals "Unsupported-property probe dataset-type lookup should fail when the destination type probe fails." \
		"1" "$status"
	assertContains "Unsupported-property probe dataset-type lookup should preserve the type-probe failure details." \
		"$output" "Failed to determine the destination property-support probe dataset type for [backup/dst]: type lookup failure"
}

test_get_unsupported_property_probe_destination_for_source_reports_missing_initial_source() {
	g_initial_source=""

	set +e
	output=$(
		(
			zxfer_get_unsupported_property_probe_destination_for_source "tank/src"
		)
	)
	status=$?

	assertEquals "Unsupported-property probe destination mapping should fail when g_initial_source is unavailable." \
		"1" "$status"
	assertContains "Unsupported-property probe destination mapping should surface the missing-initial-source helper message." \
		"$output" "Failed to determine the initial source dataset for unsupported-property probe mapping."
}

test_get_unsupported_property_probe_destination_for_source_uses_shared_destination_mapping_helper() {
	g_initial_source="tank/src"
	g_destination="backup/dst"

	output=$(
		(
			zxfer_get_destination_dataset_for_source_dataset() {
				printf '%s\n' "shared/$1"
			}
			zxfer_get_unsupported_property_probe_destination_for_source "tank/src/child"
		)
	)

	assertEquals "Unsupported-property probe destination mapping should use the shared destination-dataset helper when it is available." \
		"shared/tank/src/child" "$output"
}

test_get_unsupported_property_probe_destination_for_source_uses_literal_non_trailing_slash_mapping() {
	g_initial_source="tank/app.v1"
	g_initial_source_had_trailing_slash=0
	g_destination="backup/dst"

	assertEquals "Unsupported-property probe destination mapping should preserve dots in source dataset names as literal path components." \
		"backup/dst/app.v1/child.release" "$(zxfer_get_unsupported_property_probe_destination_for_source "tank/app.v1/child.release")"
}

test_get_unsupported_property_probe_destination_for_source_rejects_datasets_outside_initial_tree() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_destination="backup/dst"

	set +e
	output=$(
		(
			zxfer_get_unsupported_property_probe_destination_for_source "tank/srcvol"
		)
	)
	status=$?

	assertEquals "Unsupported-property probe destination mapping should fail closed when asked to map a dataset outside the initial source tree." \
		"1" "$status"
	assertContains "Unsupported-property probe destination mapping should explain the source-tree mismatch." \
		"$output" "Unsupported-property probe source dataset [tank/srcvol] is outside the initial source tree [tank/src]."
}

test_get_unsupported_property_probe_dataset_for_source_propagates_mapping_failures() {
	g_initial_source=""

	set +e
	output=$(
		(
			zxfer_get_unsupported_property_probe_dataset_for_source "tank/src"
		)
	)
	status=$?

	assertEquals "Unsupported-property probe dataset lookup should preserve destination-mapping helper failures." \
		"1" "$status"
	assertContains "Unsupported-property probe dataset lookup should surface the mapping helper failure details." \
		"$output" "Failed to determine the initial source dataset for unsupported-property probe mapping."
}

test_append_unsupported_property_for_dataset_type_appends_without_duplicates() {
	zxfer_append_unsupported_property_for_dataset_type filesystem "compression"
	zxfer_append_unsupported_property_for_dataset_type filesystem "quota"
	zxfer_append_unsupported_property_for_dataset_type filesystem "compression"

	assertEquals "Unsupported-property caches should append new properties once and ignore duplicates for one dataset type." \
		"compression,quota" "$g_zxfer_unsupported_filesystem_properties"
}

test_calculate_unsupported_properties_combines_filesystem_and_volume_unsupported_lists_into_union() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src tank/src/vol"
	g_destination="backup/dst"

	(
		zxfer_exists_destination() {
			case "$1" in
			backup/dst | backup/dst/vol)
				printf '1\n'
				;;
			*)
				printf '0\n'
				;;
			esac
		}
		zxfer_run_destination_zfs_cmd() {
			case "$*" in
			"get -Hpo value type backup/dst")
				printf 'filesystem\n'
				;;
			"get -Hpo value type backup/dst/vol")
				printf 'volume\n'
				;;
			"get -Hpo property,value,source recordsize backup/dst" | \
				"get -Hpo property,value,source volblocksize backup/dst/vol")
				printf '%s\n' "invalid property"
				return 1
				;;
			esac
		}
		zxfer_run_source_zfs_cmd() {
			case "$*" in
			"get -Hpo value type tank/src")
				printf 'filesystem\n'
				;;
			"get -Hpo property all tank/src")
				printf 'recordsize\n'
				;;
			"get -Hpo value type tank/src/vol")
				printf 'volume\n'
				;;
			"get -Hpo property all tank/src/vol")
				printf 'volblocksize\n'
				;;
			esac
		}
		zxfer_calculate_unsupported_properties
		printf '%s\n' "$g_unsupported_properties"
	) >"$TEST_TMPDIR/unsupported_union_props.out"

	assertEquals "Unsupported-property calculation should preserve both filesystem and volume entries in the compatibility union." \
		"recordsize,volblocksize" "$(cat "$TEST_TMPDIR/unsupported_union_props.out")"
}

test_calculate_unsupported_properties_scans_recursive_children_and_caches_by_dataset_type() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src tank/src/vol"
	g_destination="backup/dst"
	probe_log="$TEST_TMPDIR/unsupported_recursive_probe.log"
	: >"$probe_log"

	(
		PROBE_LOG="$probe_log"
		zxfer_exists_destination() {
			case "$1" in
			backup/dst | backup/dst/vol)
				printf '1\n'
				;;
			*)
				printf '0\n'
				;;
			esac
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$PROBE_LOG"
			case "$*" in
			"get -Hpo value type backup/dst")
				printf 'filesystem\n'
				;;
			"get -Hpo value type backup/dst/vol")
				printf 'volume\n'
				;;
			"get -Hpo property,value,source compression backup/dst")
				printf 'compression\tlz4\tlocal\n'
				;;
			"get -Hpo property,value,source volblocksize backup/dst/vol")
				printf '%s\n' "invalid property"
				return 1
				;;
			esac
		}
		zxfer_run_source_zfs_cmd() {
			case "$*" in
			"get -Hpo value type tank/src")
				printf 'filesystem\n'
				;;
			"get -Hpo property all tank/src")
				printf 'compression\n'
				;;
			"get -Hpo value type tank/src/vol")
				printf 'volume\n'
				;;
			"get -Hpo property all tank/src/vol")
				printf 'volblocksize\n'
				;;
			esac
		}
		zxfer_calculate_unsupported_properties
		printf 'union=%s\n' "$g_unsupported_properties"
		zxfer_select_unsupported_properties_for_dataset_type filesystem
		printf 'filesystem=%s\n' "$g_unsupported_properties"
		zxfer_select_unsupported_properties_for_dataset_type volume
		printf 'volume=%s\n' "$g_unsupported_properties"
	) >"$TEST_TMPDIR/unsupported_recursive_props.out"

	assertEquals "Recursive unsupported-property scans should include child dataset properties and keep the union for compatibility." \
		"union=volblocksize
filesystem=
volume=volblocksize" "$(cat "$TEST_TMPDIR/unsupported_recursive_props.out")"
	assertContains "Recursive unsupported-property scans should probe child-dataset properties that are absent from the initial source dataset." \
		"$(cat "$probe_log")" "get -Hpo property,value,source volblocksize backup/dst/vol"
}

test_calculate_unsupported_properties_does_not_mark_volume_properties_unsupported_when_pool_fallback_type_differs() {
	g_initial_source="tank/srcvol"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/srcvol"
	g_destination="backup/dstvol"
	probe_log="$TEST_TMPDIR/unsupported_volume_fallback_probe.log"
	: >"$probe_log"

	(
		PROBE_LOG="$probe_log"
		zxfer_exists_destination() {
			printf '0\n'
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$PROBE_LOG"
			case "$*" in
			"get -Hpo value type backup")
				printf 'filesystem\n'
				;;
			"get -Hpo property,value,source compression backup")
				printf 'compression\tlz4\tlocal\n'
				;;
			"get -Hpo property,value,source volblocksize backup" | "get -Hpo property,value,source refreservation backup")
				printf '%s\n' "property does not apply to datasets of this type"
				return 1
				;;
			esac
		}
		zxfer_run_source_zfs_cmd() {
			case "$*" in
			"get -Hpo value type tank/srcvol")
				printf 'volume\n'
				;;
			"get -Hpo property all tank/srcvol")
				printf 'compression\nvolblocksize\nrefreservation\n'
				;;
			esac
		}
		zxfer_calculate_unsupported_properties
		printf 'union=%s\n' "$g_unsupported_properties"
		zxfer_select_unsupported_properties_for_dataset_type volume
		printf 'volume=%s\n' "$g_unsupported_properties"
	) >"$TEST_TMPDIR/unsupported_volume_fallback.out"

	assertEquals "Pool-root fallback probes should not mark valid volume-only properties unsupported just because the fallback dataset type is filesystem." \
		"union=
volume=" "$(cat "$TEST_TMPDIR/unsupported_volume_fallback.out")"
	assertContains "Pool-root fallback probes should still inspect volume-only property names before treating the result as inconclusive." \
		"$(cat "$probe_log")" "get -Hpo property,value,source volblocksize backup"
}

test_calculate_unsupported_properties_uses_existing_child_destination_probe_dataset_types() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src tank/src/childvol"
	g_destination="backup/dst"
	probe_log="$TEST_TMPDIR/unsupported_child_destination_probe.log"
	: >"$probe_log"

	(
		PROBE_LOG="$probe_log"
		zxfer_exists_destination() {
			case "$1" in
			backup/dst | backup/dst/childvol)
				printf '1\n'
				;;
			*)
				printf '0\n'
				;;
			esac
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$PROBE_LOG"
			case "$*" in
			"get -Hpo value type backup/dst")
				printf 'filesystem\n'
				;;
			"get -Hpo value type backup/dst/childvol")
				printf 'volume\n'
				;;
			"get -Hpo property,value,source compression backup/dst")
				printf 'compression\tlz4\tlocal\n'
				;;
			"get -Hpo property,value,source volblocksize backup/dst")
				printf '%s\n' "property does not apply to datasets of this type"
				return 1
				;;
			"get -Hpo property,value,source volblocksize backup/dst/childvol")
				printf '%s\n' "invalid property"
				return 1
				;;
			esac
		}
		zxfer_run_source_zfs_cmd() {
			case "$*" in
			"get -Hpo value type tank/src")
				printf 'filesystem\n'
				;;
			"get -Hpo property all tank/src")
				printf 'compression\n'
				;;
			"get -Hpo value type tank/src/childvol")
				printf 'volume\n'
				;;
			"get -Hpo property all tank/src/childvol")
				printf 'volblocksize\n'
				;;
			esac
		}
		zxfer_calculate_unsupported_properties
		printf 'union=%s\n' "$g_unsupported_properties"
		zxfer_select_unsupported_properties_for_dataset_type volume
		printf 'volume=%s\n' "$g_unsupported_properties"
	) >"$TEST_TMPDIR/unsupported_child_destination_probe.out"

	assertEquals "Recursive unsupported-property scans should probe against an existing child destination dataset when its type differs from the destination root." \
		"union=volblocksize
volume=volblocksize" "$(cat "$TEST_TMPDIR/unsupported_child_destination_probe.out")"
	assertContains "Existing child destination datasets should be used as the unsupported-property probe target for matching source datasets." \
		"$(cat "$probe_log")" "get -Hpo property,value,source volblocksize backup/dst/childvol"
}

test_calculate_unsupported_properties_retries_inconclusive_probes_until_one_is_authoritative() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src/vol-missing tank/src/vol-existing"
	g_destination="backup/dst"
	probe_log="$TEST_TMPDIR/unsupported_inconclusive_retry.log"
	: >"$probe_log"

	(
		PROBE_LOG="$probe_log"
		zxfer_exists_destination() {
			case "$1" in
			backup/dst/vol-existing)
				printf '1\n'
				;;
			*)
				printf '0\n'
				;;
			esac
		}
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$*" >>"$PROBE_LOG"
			case "$*" in
			"get -Hpo value type backup")
				printf 'filesystem\n'
				;;
			"get -Hpo value type backup/dst/vol-existing")
				printf 'volume\n'
				;;
			"get -Hpo property,value,source volblocksize backup")
				printf '%s\n' "property does not apply to datasets of this type"
				return 1
				;;
			"get -Hpo property,value,source volblocksize backup/dst/vol-existing")
				printf '%s\n' "invalid property"
				return 1
				;;
			esac
		}
		zxfer_run_source_zfs_cmd() {
			case "$*" in
			"get -Hpo value type tank/src/vol-missing" | "get -Hpo value type tank/src/vol-existing")
				printf 'volume\n'
				;;
			"get -Hpo property all tank/src/vol-missing" | "get -Hpo property all tank/src/vol-existing")
				printf 'volblocksize\n'
				;;
			esac
		}
		zxfer_calculate_unsupported_properties
		printf 'union=%s\n' "$g_unsupported_properties"
		zxfer_select_unsupported_properties_for_dataset_type volume
		printf 'volume=%s\n' "$g_unsupported_properties"
	) >"$TEST_TMPDIR/unsupported_inconclusive_retry.out"

	assertEquals "Inconclusive unsupported-property probes should not prevent a later authoritative probe for the same dataset type and property." \
		"union=volblocksize
volume=volblocksize" "$(cat "$TEST_TMPDIR/unsupported_inconclusive_retry.out")"
	assertContains "Later matching-type destinations should still be probed after an earlier pool-root fallback was inconclusive." \
		"$(cat "$probe_log")" "get -Hpo property,value,source volblocksize backup/dst/vol-existing"
}

test_calculate_unsupported_properties_fails_closed_on_destination_probe_error() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"

	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '0\n'
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$*" = "get -Hpo value type backup" ]; then
					printf 'filesystem\n'
					return 0
				fi
				printf '%s\n' "ssh failure"
				return 1
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$*" = "get -Hpo value type tank/src" ]; then
					printf 'filesystem\n'
					return 0
				fi
				if [ "$*" = "get -Hpo property all tank/src" ]; then
					printf 'compression\n'
				fi
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_calculate_unsupported_properties
		) 2>&1
	)
	status=$?

	assertEquals "Destination capability probe failures should abort unsupported-property calculation." \
		"1" "$status"
	assertContains "Destination capability probe failures should be surfaced instead of stripping all properties." \
		"$output" "Failed to probe destination support for property [compression] on [backup]: ssh failure"
}

test_calculate_unsupported_properties_reports_blank_destination_probe_failures_when_destination_query_fails_too() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"

	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '0\n'
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$*" = "get -Hpo value type backup" ]; then
					printf 'filesystem\n'
					return 0
				fi
				if [ "$*" = "get -Hpo property,value,source compression backup" ]; then
					return 1
				fi
				if [ "$*" = "get -Hpo property all backup" ]; then
					return 1
				fi
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$*" = "get -Hpo value type tank/src" ]; then
					printf 'filesystem\n'
					return 0
				fi
				if [ "$*" = "get -Hpo property all tank/src" ]; then
					printf 'compression\n'
				fi
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_calculate_unsupported_properties
		) 2>&1
	)
	status=$?

	assertEquals "Blank destination property-probe failures should still abort unsupported-property calculation when the destination dataset is not otherwise queryable." \
		"1" "$status"
	assertContains "Blank destination property-probe failures should surface a non-empty fallback diagnostic." \
		"$output" "Failed to probe destination support for property [compression] on [backup]: probe exited nonzero without stdout/stderr"
}

test_calculate_unsupported_properties_fails_closed_on_source_type_probe_error() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"

	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$*" = "get -Hpo value type backup/dst" ]; then
					printf 'filesystem\n'
				fi
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$*" = "get -Hpo value type tank/src" ]; then
					printf '%s\n' "source type failure"
					return 1
				fi
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_calculate_unsupported_properties
		) 2>&1
	)
	status=$?

	assertEquals "Source dataset-type probe failures should abort unsupported-property calculation." \
		"1" "$status"
	assertContains "Source dataset-type probe failures should preserve the new unsupported-scan error context." \
		"$output" "Failed to retrieve source dataset type for unsupported-property scan [tank/src]: source type failure"
}

test_calculate_unsupported_properties_fails_closed_on_source_probe_error() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"

	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$*" = "get -Hpo value type backup/dst" ]; then
					printf 'filesystem\n'
				fi
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$*" = "get -Hpo value type tank/src" ]; then
					printf 'filesystem\n'
					return 0
				fi
				if [ "$*" = "get -Hpo property all tank/src" ]; then
					printf '%s\n' "local failure"
					return 1
				fi
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_calculate_unsupported_properties
		) 2>&1
	)
	status=$?

	assertEquals "Source capability probe failures should abort unsupported-property calculation." \
		"1" "$status"
	assertContains "Source capability probe failures should be surfaced instead of silently preserving all properties." \
		"$output" "Failed to retrieve source property list for dataset [tank/src]: local failure"
}

test_calculate_unsupported_properties_rethrows_tempfile_allocation_failures() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"

	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_run_destination_zfs_cmd() {
				if [ "$*" = "get -Hpo value type backup/dst" ]; then
					printf 'filesystem\n'
					return 0
				fi
				printf '%s\n' "unexpected destination probe $*"
				return 1
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$*" = "get -Hpo value type tank/src" ]; then
					printf 'filesystem\n'
					return 0
				fi
				if [ "$*" = "get -Hpo property all tank/src" ]; then
					printf '%s\n' "compression"
					return 0
				fi
				printf '%s\n' "unexpected source probe $*"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_get_temp_file() {
				zxfer_throw_error "Error creating temporary file."
			}
			zxfer_calculate_unsupported_properties
		) 2>&1
	)
	status=$?

	assertEquals "Unsupported-property scanning should fail closed when temp-file allocation fails." \
		"1" "$status"
	assertEquals "Unsupported-property scanning should preserve the temp-file allocation failure." \
		"Error creating temporary file." "$output"
}

test_calculate_unsupported_properties_fails_closed_on_staged_source_property_readback_error() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"
	probe_log="$TEST_TMPDIR/unsupported_property_readback_probe.log"
	: >"$probe_log"

	set +e
	output=$(
		(
			PROBE_LOG="$probe_log"
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$*" >>"$PROBE_LOG"
				if [ "$*" = "get -Hpo value type backup/dst" ]; then
					printf 'filesystem\n'
					return 0
				fi
				printf '%s\n' "unexpected destination probe $*"
				return 1
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$*" = "get -Hpo value type tank/src" ]; then
					printf 'filesystem\n'
					return 0
				fi
				if [ "$*" = "get -Hpo property all tank/src" ]; then
					printf '%s\n' "compression\nchecksum"
					return 0
				fi
				printf '%s\n' "unexpected source probe $*"
				return 1
			}
			call_count=0
			zxfer_get_temp_file() {
				call_count=$((call_count + 1))
				g_zxfer_temp_file_result="$TEST_TMPDIR/unsupported-readback-$call_count.tmp"
				: >"$g_zxfer_temp_file_result"
				return 0
			}
			cat() {
				if [ "$1" = "$TEST_TMPDIR/unsupported-readback-1.tmp" ]; then
					printf '%s\n' "read failed" >&2
					return 9
				fi
				command cat "$@"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_calculate_unsupported_properties
		) 2>&1
	)
	status=$?

	assertEquals "Unsupported-property scanning should fail closed when the staged source property list cannot be read back." \
		"1" "$status"
	assertContains "Unsupported-property staged readback failures should preserve the underlying readback diagnostic." \
		"$output" "read failed"
	assertContains "Unsupported-property staged readback failures should report unsupported-scan context." \
		"$output" "Failed to read staged source property list for unsupported-property scan [tank/src]."
	assertEquals "Unsupported-property staged readback failures should stop before destination support probes begin." \
		"get -Hpo value type backup/dst" "$(cat "$probe_log")"
}

test_ensure_required_properties_present_appends_missing_creation_time_props() {
	result=$(
		(
			zxfer_run_zfs_cmd_for_spec() {
				if [ "$5" = "casesensitivity" ]; then
					printf 'casesensitivity\tsensitive\tlocal\n'
					return 0
				fi
				printf '%s\n' "invalid property"
				return 1
			}
			zxfer_ensure_required_properties_present "tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity,utf8only"
		)
	)

	assertEquals "Missing required creation-time properties should be appended from explicit zfs get queries." \
		"compression=lz4=local,casesensitivity=sensitive=local" "$result"
}

test_ensure_required_properties_present_appends_missing_creation_time_props_to_empty_lists() {
	result=$(
		(
			g_LZFS="/default/source/zfs"
			zxfer_run_zfs_cmd_for_spec() {
				if [ "$1" != "/default/source/zfs" ]; then
					printf '%s\n' "unexpected zfs command: $1"
					return 1
				fi
				printf 'casesensitivity\tsensitive\tlocal\n'
			}
			zxfer_ensure_required_properties_present "tank/src" "" "" "casesensitivity"
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
		zxfer_run_zfs_cmd_for_spec() {
			if [ "$5" = "casesensitivity" ]; then
				printf 'casesensitivity\tsensitive\tlocal\n'
				return 0
			fi
			printf '%s\n' "not supported"
			return 1
		}
		zxfer_ensure_required_properties_present "tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity,utf8only" >/dev/null
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

	zxfer_run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		if [ "$5" = "casesensitivity" ]; then
			printf 'casesensitivity\tsensitive\tlocal\n'
			return 0
		fi
		printf '%s\n' "not supported"
		return 1
	}

	zxfer_ensure_required_properties_present "tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity,utf8only" source >"$log"
	zxfer_ensure_required_properties_present "tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity,utf8only" source >>"$log"
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

test_zxfer_get_required_property_probe_caches_unsupported_results() {
	calls_log="$TEST_TMPDIR/required_probe_unsupported.calls"
	: >"$calls_log"

	zxfer_run_zfs_cmd_for_spec() {
		printf 'call\n' >>"$calls_log"
		printf '%s\n' "property does not apply to datasets of this type"
		return 1
	}

	zxfer_get_required_property_probe "tank/vol" "casesensitivity" "/sbin/zfs" source
	first_result=$g_zxfer_required_property_probe_result
	zxfer_get_required_property_probe "tank/vol" "casesensitivity" "/sbin/zfs" source
	second_result=$g_zxfer_required_property_probe_result

	unset -f zxfer_run_zfs_cmd_for_spec

	assertEquals "Unsupported required-property probes should use the sentinel result." \
		"__ZXFER_REQUIRED_PROPERTY_UNSUPPORTED__" "$first_result"
	assertEquals "Unsupported required-property probe cache entries should be reused on subsequent reads." \
		"__ZXFER_REQUIRED_PROPERTY_UNSUPPORTED__" "$second_result"
	assertEquals "Unsupported required-property probe results should be cached by dataset and side." \
		"1" "$(awk 'END {print NR + 0}' "$calls_log")"
}

test_ensure_required_properties_present_skips_nonapplicable_creation_time_props() {
	result=$(
		(
			zxfer_run_zfs_cmd_for_spec() {
				printf '%s\n' "cannot get property: property does not apply to datasets of this type"
				return 1
			}
			zxfer_ensure_required_properties_present "tank/vol" "compression=lz4=local" "/sbin/zfs" "casesensitivity,utf8only"
		)
	)

	assertEquals "Explicit must-create probes that clearly do not apply to the dataset type should be skipped." \
		"compression=lz4=local" "$result"
}

test_ensure_required_properties_present_reports_parse_failures_for_malformed_probe_output() {
	set +e
	output=$(
		(
			zxfer_run_zfs_cmd_for_spec() {
				printf 'casesensitivity\tinvalid\n'
				return 0
			}
			zxfer_ensure_required_properties_present "tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity"
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
			zxfer_run_zfs_cmd_for_spec() {
				printf '%s\n' "permission denied"
				return 1
			}
			zxfer_ensure_required_properties_present "tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity"
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
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "unexpected $*"
					return 1
				fi
			}
			zxfer_get_validated_source_dataset_create_metadata "tank/src"
		)
	)

	assertEquals "Filesystem metadata validation should return the type and a blank volume size." \
		"filesystem" "$result"
}

test_get_validated_source_dataset_create_metadata_reports_type_probe_failures() {
	set +e
	output=$(
		(
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "permission denied"
				return 1
			}
			zxfer_get_validated_source_dataset_create_metadata "tank/src"
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
			zxfer_run_source_zfs_cmd() {
				printf '%s\n' "snapshot"
			}
			zxfer_get_validated_source_dataset_create_metadata "tank/src"
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
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "volume"
				elif [ "$4" = "volsize" ]; then
					printf '\n'
				else
					printf '%s\n' "unexpected $*"
					return 1
				fi
			}
			zxfer_get_validated_source_dataset_create_metadata "tank/vol"
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
			zxfer_run_source_zfs_cmd() {
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
			zxfer_get_validated_source_dataset_create_metadata "tank/vol"
		)
	)
	status=$?

	assertEquals "Volume metadata validation should abort on volsize probe failures." 1 "$status"
	assertContains "Volsize probe failures should identify the source zvol." \
		"$output" "Failed to retrieve source zvol size for [tank/vol]: ssh timeout"
}

test_get_required_creation_properties_for_dataset_type_skips_filesystem_only_props_for_volumes() {
	assertEquals "Volumes should not probe filesystem-only must-create properties." \
		"" "$(zxfer_get_required_creation_properties_for_dataset_type "volume")"
	assertEquals "Filesystems should continue to enforce must-create creation properties." \
		"casesensitivity,normalization,utf8only" "$(zxfer_get_required_creation_properties_for_dataset_type "filesystem")"
}

test_ensure_destination_exists_returns_one_when_dataset_already_exists() {
	set +e
	zxfer_ensure_destination_exists 1 1 "" "" filesystem "" "backup/dst" "$g_test_base_readonly_properties" ""
	status=$?

	assertEquals "Existing destinations should skip creation and return 1." 1 "$status"
}

test_ensure_destination_exists_initial_source_adds_parents_when_missing() {
	result=$(
		(
			zxfer_exists_destination() {
				printf '0\n'
			}
			create_runner() {
				printf '%s|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" "$5"
			}
			zxfer_ensure_destination_exists 0 1 "compression=lz4=local,atime=off=override" "" filesystem "" "backup/dst/child" "$g_test_base_readonly_properties" create_runner
		)
	)

	assertEquals "Initial-source creation should add parents when the parent dataset is missing." \
		"yes|filesystem||compression=lz4,atime=off|backup/dst/child" "$result"
}

test_ensure_destination_exists_reports_parent_probe_failures() {
	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '%s\n' "Failed to determine whether destination dataset [backup/dst] exists: permission denied"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "$g_test_base_readonly_properties" create_runner
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
			zxfer_ensure_destination_exists 0 0 "" "mountpoint=/mnt=local,readonly=off=local,compression=lz4=local" filesystem "" "backup/dst/child" "readonly" create_runner
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
			zxfer_exists_destination() {
				printf '0\n'
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst" "$g_test_base_readonly_properties" create_runner
		)
	)
	status=$?

	assertEquals "Create-runner failures should abort destination creation." 1 "$status"
	assertContains "Create-runner failures should use the destination-creation error." \
		"$output" "Error when creating destination filesystem."
}

test_ensure_destination_exists_uses_default_runner_when_unspecified_in_current_shell() {
	log="$TEST_TMPDIR/default_create_runner.log"
	zxfer_run_zfs_create_with_properties() {
		printf '%s|%s|%s|%s|%s\n' "$1" "$2" "$3" "$4" "$5" >"$log"
	}
	g_test_base_readonly_properties=""
	g_option_I_ignore_properties=""

	zxfer_ensure_destination_exists 0 0 "" "readonly=off=local,compression=lz4=local" filesystem "" "backup/dst/child" "readonly" ""
	status=$?

	unset -f zxfer_run_zfs_create_with_properties

	assertEquals "Blank create-runner arguments should fall back to the default zfs create helper." 0 "$status"
	assertEquals "Default create-runner selection should sanitize creation properties using the supplied readonly list before invocation." \
		"yes|filesystem||compression=lz4|backup/dst/child" "$(cat "$log")"
}

test_ensure_destination_exists_marks_created_hierarchy_in_cache() {
	# shellcheck disable=SC2030
	output=$(
		g_recursive_dest_list=""
		zxfer_mark_destination_root_missing_in_cache "backup/dst"
		zxfer_exists_destination() {
			printf '0\n'
		}
		create_runner() {
			return 0
		}
		zxfer_ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "$g_test_base_readonly_properties" create_runner
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
		zxfer_exists_destination() {
			printf '1\n'
		}
		create_runner() {
			return 0
		}
		zxfer_ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "$g_test_base_readonly_properties" create_runner
		printf 'dests=%s\n' "$g_recursive_dest_list"
	)

	assertContains "Appending a created dataset under an already-tracked destination root should preserve exact newline-delimited dataset names without leading whitespace." \
		"$output" "dests=backup/dst
backup/dst/child"
}

test_collect_destination_props_defaults_to_g_rzfs() {
	output_file="$TEST_TMPDIR/collect_destination_props_default.out"

	zxfer_load_destination_props() {
		g_zxfer_destination_pvs_raw="$1|${2:-$g_RZFS}"
	}
	g_RZFS="/remote/zfs"
	zxfer_collect_destination_props "backup/dst" "" >"$output_file"
	# shellcheck source=src/zxfer_property_cache.sh
	. "$ZXFER_ROOT/src/zxfer_property_cache.sh"
	# shellcheck source=src/zxfer_property_reconcile.sh
	. "$ZXFER_ROOT/src/zxfer_property_reconcile.sh"

	assertEquals "Destination property collection should default to g_RZFS." \
		"backup/dst|/remote/zfs" "$(cat "$output_file")"
}

test_load_destination_props_defaults_to_g_rzfs_and_records_raw_props() {
	output_file="$TEST_TMPDIR/load_destination_props_default.out"

	zxfer_load_normalized_dataset_properties() {
		printf 'dataset=%s|zfs=%s|side=%s\n' "$1" "$2" "$3" >"$output_file"
		g_zxfer_normalized_dataset_properties="compression=lz4=local"
	}
	g_RZFS="/remote/zfs"
	zxfer_load_destination_props "backup/dst" ""
	printf 'raw=%s\n' "$g_zxfer_destination_pvs_raw" >>"$output_file"
	# shellcheck source=src/zxfer_property_cache.sh
	. "$ZXFER_ROOT/src/zxfer_property_cache.sh"
	# shellcheck source=src/zxfer_property_reconcile.sh
	. "$ZXFER_ROOT/src/zxfer_property_reconcile.sh"

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
			zxfer_load_destination_props "backup/dst" ""
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
		zxfer_exists_destination() {
			printf '1\n'
		}
		create_runner() {
			return 0
		}
		zxfer_invalidate_destination_property_cache() {
			printf '%s\n' "$1" >>"$log"
		}
		zxfer_ensure_destination_exists 0 1 "compression=lz4=local" "" filesystem "" "backup/dst" "$g_test_base_readonly_properties" create_runner
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
			zxfer_run_destination_zfs_cmd() {
				return 1
			}
			zxfer_throw_error() {
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

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$@" >"$log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_RZFS="/sbin/zfs"
	zxfer_run_zfs_set_property "$l_property" "$l_value" "backup/dst"

	unset -f zxfer_run_destination_zfs_cmd

	assertEquals "Live local property sets should pass the literal assignment to zfs without shell-style escaping." \
		"$(printf '%s\n' "set" "$l_property=$l_value" "backup/dst")" "$(cat "$log")"
}

test_zxfer_run_zfs_set_property_invalidates_destination_cache_after_live_set() {
	log="$TEST_TMPDIR/set_invalidation.log"
	: >"$log"

	(
		zxfer_run_destination_zfs_cmd() {
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

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$@" >"$log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_RZFS="/sbin/zfs"
	zxfer_run_zfs_set_properties "compression=lz4,atime=off" "backup/dst"

	unset -f zxfer_run_destination_zfs_cmd

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
		zxfer_run_destination_zfs_cmd() {
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

	zxfer_run_destination_zfs_cmd() {
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

	unset -f zxfer_run_destination_zfs_cmd
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
while [ $# -gt 0 ]; do
	case "$1" in
	-o | -S | -O)
		shift 2
		;;
	-M | -N | -fN)
		shift
		;;
	--)
		shift
		break
		;;
	-*)
		shift
		;;
	*)
		break
		;;
	esac
done
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
if ! eval "set -- $remote_cmd"; then
	exit 1
fi
"$@"
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
while [ $# -gt 0 ]; do
	case "$1" in
	-o | -S | -O)
		shift 2
		;;
	-M | -N | -fN)
		shift
		;;
	--)
		shift
		break
		;;
	-*)
		shift
		;;
	*)
		break
		;;
	esac
done
host=$1
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
while [ $# -gt 0 ]; do
	case "$1" in
	-o | -S | -O)
		shift 2
		;;
	-M | -N | -fN)
		shift
		;;
	--)
		shift
		break
		;;
	-*)
		shift
		;;
	*)
		break
		;;
	esac
done
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
if ! eval "set -- $remote_cmd"; then
	exit 1
fi
"$@"
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
while [ $# -gt 0 ]; do
	case "$1" in
	-o | -S | -O)
		shift 2
		;;
	-M | -N | -fN)
		shift
		;;
	--)
		shift
		break
		;;
	-*)
		shift
		;;
	*)
		break
		;;
	esac
done
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
if ! eval "set -- $remote_cmd"; then
	exit 1
fi
"$@"
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

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$@" >"$log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	zxfer_run_zfs_set_properties "user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi" "backup/dst"

	assertEquals "Batched property sets should decode delimiter-heavy values before invoking zfs set." \
		"$(printf '%s\n' "set" "user:note=value,with,commas=and;semi" "backup/dst")" "$(cat "$log")"

	unset -f zxfer_run_destination_zfs_cmd
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
			zxfer_run_destination_zfs_cmd() {
				return 1
			}
			zxfer_throw_error() {
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
		zxfer_run_destination_zfs_cmd() {
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

	zxfer_run_destination_zfs_cmd() {
		printf '%s\n' "$@" >"$log"
	}

	g_option_n_dryrun=0
	g_option_T_target_host=""
	g_RZFS="/sbin/zfs"
	zxfer_run_zfs_inherit_property "$l_property" "backup/dst"

	unset -f zxfer_run_destination_zfs_cmd

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
while [ $# -gt 0 ]; do
	case "$1" in
	-o | -S | -O)
		shift 2
		;;
	-M | -N | -fN)
		shift
		;;
	--)
		shift
		break
		;;
	-*)
		shift
		;;
	*)
		break
		;;
	esac
done
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
if ! eval "set -- $remote_cmd"; then
	exit 1
fi
"$@"
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
			zxfer_throw_error_with_usage() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_diff_properties "casesensitivity=mixed=local" "casesensitivity=sensitive=local" "casesensitivity"
		)
	)
	status=$?

	assertEquals "Must-create property mismatches should abort." 1 "$status"
	assertContains "Must-create mismatches should explain that the property may only be set at creation time." \
		"$output" "may only be set"
}

test_diff_properties_sets_local_value_when_destination_source_is_inherited() {
	outfile="$TEST_TMPDIR/diff_set_local.out"

	zxfer_diff_properties "compression=lz4=local" "compression=lz4=inherited" "" >"$outfile"

	assertEquals "Initial-source property sets should still include matching values when the destination source is not local." \
		"compression=lz4" "$(sed -n '1p' "$outfile")"
	assertEquals "Child property sets should force local values when the destination inherits the same value." \
		"compression=lz4" "$(sed -n '2p' "$outfile")"
	assertEquals "No inherit list should be produced when the source is already local." \
		"" "$(sed -n '3p' "$outfile")"
}

test_diff_properties_inherits_value_when_destination_is_local_but_source_is_not() {
	outfile="$TEST_TMPDIR/diff_inherit_same_value.out"

	zxfer_diff_properties "compression=lz4=inherited" "compression=lz4=local" "" >"$outfile"

	assertEquals "No initial-source set list is needed when the destination already has the matching local value." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "No child set list should be produced when the source value is inherited." \
		"" "$(sed -n '2p' "$outfile")"
	assertEquals "Child property diffs should request inheritance when the destination has a local copy of an inherited source value." \
		"compression=lz4" "$(sed -n '3p' "$outfile")"
}

test_diff_properties_treats_overrides_as_local_sets() {
	outfile="$TEST_TMPDIR/diff_override_local.out"

	zxfer_diff_properties "quota=32M=override" "quota=32M=local" "" >"$outfile"

	assertEquals "Matching local override values should not request any additional root-level set." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "Matching local override values should not request a child set." \
		"" "$(sed -n '2p' "$outfile")"
	assertEquals "Matching local override values must not be converted into inheritance requests." \
		"" "$(sed -n '3p' "$outfile")"

	zxfer_diff_properties "quota=32M=override" "quota=none=local" "" >"$outfile"

	assertEquals "Changed override values should still appear in the root set list." \
		"quota=32M" "$(sed -n '1p' "$outfile")"
	assertEquals "Changed override values should be set locally on child datasets." \
		"quota=32M" "$(sed -n '2p' "$outfile")"
	assertEquals "Changed override values must not be converted into inheritance requests." \
		"" "$(sed -n '3p' "$outfile")"
}

test_diff_properties_sets_missing_override_properties_locally() {
	outfile="$TEST_TMPDIR/diff_override_missing_dest.out"

	zxfer_diff_properties "user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=override" "compression=lz4=local" "" >"$outfile"

	assertEquals "Destination properties missing an override-managed property should still request a root-level local set." \
		"user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi" "$(sed -n '1p' "$outfile")"
	assertEquals "Destination properties missing an override-managed property should still request a child local set." \
		"user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi" "$(sed -n '2p' "$outfile")"
	assertEquals "Missing override-managed properties must not be converted into inheritance requests." \
		"" "$(sed -n '3p' "$outfile")"
}

test_diff_properties_reports_awk_failures() {
	set +e
	output=$(
		(
			g_cmd_awk="false"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$TEST_TMPDIR/diff_awk_failure.tmp"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_diff_properties "compression=lz4=local" "compression=lz4=local" ""
		) 2>&1
	)
	status=$?

	assertEquals "Property diffing should fail closed when its awk helper cannot execute." \
		"1" "$status"
	assertContains "Property diffing awk failures should surface the helper failure message." \
		"$output" "Failed to diff dataset properties."
}

test_diff_properties_preserves_staged_readback_failures_without_publishing_results() {
	set +e
	output=$(
		(
			l_tmp_path="$TEST_TMPDIR/diff_readback_failure.tmp"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$l_tmp_path
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_read_property_reconcile_stage_file() {
				return 1
			}
			zxfer_diff_properties "compression=lz4=local" "compression=lz4=local" "" || {
				if [ -e "$l_tmp_path" ]; then
					printf 'tmp_exists=yes\n'
				else
					printf 'tmp_exists=no\n'
				fi
				exit 1
			}
		)
	)
	status=$?

	assertEquals "Property diffing should fail closed when staged diff readback fails." \
		"1" "$status"
	assertContains "Property diff staged readback failures should still clean the staged temp file." \
		"$output" "tmp_exists=no"
}

test_diff_properties_rethrows_tempfile_allocation_failures() {
	set +e
	output=$(
		(
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_get_temp_file() {
				zxfer_throw_error "Error creating temporary file."
			}
			zxfer_diff_properties "compression=lz4=local" "compression=lz4=local" ""
		) 2>&1
	)
	status=$?

	assertEquals "Property diffing should fail closed when temp-file allocation fails." \
		"1" "$status"
	assertEquals "Property diffing should preserve the temp-file allocation failure." \
		"Error creating temporary file." "$output"
}

test_adjust_child_inherit_to_match_parent_promotes_mismatched_parent_values_to_sets() {
	outfile="$TEST_TMPDIR/adjust_child_inherit.out"

	(
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "checksum=sha256=local,atime=on=local"
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
			"checksum=sha256=inherited,atime=off=inherited" \
			"quota=32M" \
			"checksum=sha256,atime=off" \
			"$g_test_base_readonly_properties"
	) >"$outfile"

	assertEquals "Parent-matching inherited properties should remain in the inherit list." \
		"quota=32M,atime=off" "$(sed -n '1p' "$outfile")"
	assertEquals "Only properties whose parent already matches should remain inherited." \
		"checksum=sha256" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_preserves_inherit_when_parent_matches() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_match.out"

	(
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "checksum=sha256=local,atime=off=local"
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
			"checksum=sha256=inherited,atime=off=inherited" \
			"" \
			"checksum=sha256,atime=off" \
			"$g_test_base_readonly_properties"
	) >"$outfile"

	assertEquals "When the parent already has the desired values, no local sets are needed." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "Matching parent values should preserve inheritance requests." \
		"checksum=sha256,atime=off" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_moves_inherited_source_properties_out_of_set_list_when_parent_matches() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_demote.out"

	(
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "checksum=sha256=local,compression=lz4=local"
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
			"checksum=sha256=inherited,compression=lz4=local" \
			"checksum=sha256,compression=lz4" \
			"" \
			"$g_test_base_readonly_properties"
	) >"$outfile"

	assertEquals "Inherited source properties should be removed from the child set list when the parent already provides the same value." \
		"compression=lz4" "$(sed -n '1p' "$outfile")"
	assertEquals "Inherited source properties whose parent already matches should be converted back into inherit operations." \
		"checksum=sha256" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_uses_supplied_readonly_list() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_readonly.out"

	(
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "compression=lz4=local,atime=off=local"
		}
		g_test_base_readonly_properties=""
		g_option_I_ignore_properties=""
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
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
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "compression=lz4=local"
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
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

	zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" "" "" "" "" >"$outfile"

	assertEquals "Child-inherit adjustment should leave an empty set list unchanged when there is nothing to reconcile." \
		"" "$(sed -n '1p' "$outfile")"
	assertEquals "Child-inherit adjustment should leave an empty inherit list unchanged when there is nothing to reconcile." \
		"" "$(sed -n '2p' "$outfile")"
}

test_adjust_child_inherit_to_match_parent_returns_unchanged_lists_for_root_dataset() {
	outfile="$TEST_TMPDIR/adjust_child_inherit_root.out"

	zxfer_adjust_child_inherit_to_match_parent "backup" \
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
		zxfer_exists_destination() {
			printf '0\n'
		}
		zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
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
			zxfer_exists_destination() {
				printf '%s\n' "ssh timeout"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
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
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_collect_destination_props() {
				printf '%s\n' "ssh timeout"
				return 1
			}
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
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

test_adjust_child_inherit_to_match_parent_rethrows_tempfile_allocation_failures() {
	set +e
	output=$(
		(
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_get_temp_file() {
				zxfer_throw_error "Error creating temporary file."
			}
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
				"compression=lz4=inherited" \
				"" \
				"compression=lz4" \
				""
		) 2>&1
	)
	status=$?

	assertEquals "Child-inherit reconciliation should fail closed when temp-file allocation fails." \
		"1" "$status"
	assertEquals "Child-inherit reconciliation should preserve the temp-file allocation failure." \
		"Error creating temporary file." "$output"
}

test_adjust_child_inherit_to_match_parent_reports_awk_failures() {
	set +e
	output=$(
		(
			g_cmd_awk="false"
			zxfer_exists_destination() {
				printf '1\n'
			}
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$TEST_TMPDIR/adjust_inherit_awk_failure.tmp"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_collect_destination_props() {
				printf '%s\n' "compression=lz4=local"
			}
			zxfer_sanitize_property_list() {
				printf '%s\n' "$1"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_adjust_child_inherit_to_match_parent "backup/dst/child" \
				"compression=lz4=inherited" \
				"" \
				"compression=lz4" \
				""
		) 2>&1
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
		zxfer_apply_property_changes "backup/dst" 0 "" "compression=lz4,atime=off" "quota=none" "" ""
	)

	assertEquals "Default property runners should be used when no custom runner is supplied." \
		"set compression=lz4,atime=off backup/dst
inherit quota backup/dst" "$(cat "$log")"
}

test_apply_property_changes_logs_when_child_only_inherits() {
	log="$TEST_TMPDIR/apply_inherit_only.log"
	: >"$log"

	(
		zxfer_echov() {
			printf '%s\n' "$*" >>"$log"
		}
		inherit_runner() {
			printf 'inherit %s %s\n' "$1" "$2" >>"$log"
		}

		zxfer_apply_property_changes "backup/dst" 0 "" "" "quota=none" "" inherit_runner
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
		zxfer_echov() {
			printf '%s\n' "$*" >>"$log"
		}
		set_runner() {
			:
		}
		inherit_runner() {
			:
		}

		zxfer_apply_property_changes "backup/dst" 0 "" \
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
		zxfer_apply_property_changes "backup/dst/child" 0 "" "compression=lz4,atime=off" "quota=none" set_runner inherit_runner
	)

	assertEquals "Child-property application should batch all set operations into one set-runner call while still inheriting properties one at a time." \
		"set compression=lz4,atime=off backup/dst/child
inherit quota backup/dst/child" "$(cat "$log")"
}

test_transfer_properties_fails_when_source_property_collection_fails() {
	set +e
	output=$(
		(
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="permission denied"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_transfer_properties "tank/src"
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
		zxfer_collect_source_props() {
			g_zxfer_source_pvs_raw="compression=lz4=local"
			g_zxfer_source_pvs_effective="compression=lz4=local"
		}
		zxfer_run_source_zfs_cmd() {
			if [ "$4" = "type" ]; then
				printf '%s\n' "filesystem"
			else
				printf '%s\n' "-"
			fi
		}
		zxfer_validate_override_properties() {
			printf 'validate %s %s\n' "$1" "$2" >>"$LOG_FILE"
		}
		zxfer_derive_override_lists() {
			g_zxfer_override_pvs_result="compression=lz4=local"
			g_zxfer_creation_pvs_result=""
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_strip_unsupported_properties() {
			printf '%s\n' "$1"
		}
		zxfer_ensure_required_properties_present() {
			printf '%s\n' "$2"
		}
		zxfer_ensure_destination_exists() {
			printf 'ensure %s\n' "$2" >>"$LOG_FILE"
			return 0
		}
		zxfer_append_backup_metadata_record() {
			printf 'backup_append %s %s %s\n' "$1" "$2" "$3" >>"$LOG_FILE"
			g_backup_file_contents="helper-owned"
		}
		zxfer_write_backup_properties() {
			printf 'unexpected backup_write %s\n' "$g_backup_file_contents" >>"$LOG_FILE"
		}
		g_option_k_backup_property_mode=1
		g_initial_source="tank/src"
		g_actual_dest="backup/dst"
		zxfer_transfer_properties "tank/src"
		printf 'created=%s\n' "$g_dest_created_by_zxfer" >>"$LOG_FILE"
		printf 'backup=%s\n' "$g_backup_file_contents" >>"$LOG_FILE"
	)

	result=$(cat "$log")
	assertContains "Initial-source transfer should validate override properties." \
		"$result" "validate  compression=lz4=local"
	assertContains "Successful destination creation should mark the dataset as zxfer-created." \
		"$result" "created=1"
	assertContains "Backup mode should append raw source properties through the backup-metadata owner helper." \
		"$result" "backup_append tank/src backup/dst compression=lz4=local"
	assertNotContains "Property reconciliation should not flush backup metadata directly; replication orchestration owns the live write timing." \
		"$result" "unexpected backup_write"
	assertContains "Backup accumulation state should remain helper-owned." \
		"$result" "backup=helper-owned"
}

test_transfer_properties_does_not_capture_backup_metadata_before_success() {
	set +e
	output=$(
		(
			append_log="$TEST_TMPDIR/transfer_failed_backup_capture.log"
			: >"$append_log"
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="compression=lz4=local"
				g_zxfer_source_pvs_effective="compression=lz4=local"
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			zxfer_validate_override_properties() {
				:
			}
			zxfer_derive_override_lists() {
				g_zxfer_override_pvs_result="compression=lz4=local"
				g_zxfer_creation_pvs_result=""
			}
			zxfer_sanitize_property_list() {
				printf '%s\n' "$1"
			}
			zxfer_strip_unsupported_properties() {
				printf '%s\n' "$1"
			}
			zxfer_ensure_required_properties_present() {
				printf '%s\n' "$2"
			}
			zxfer_ensure_destination_exists() {
				return 1
			}
			zxfer_collect_destination_props() {
				printf '%s\n' "destination lookup failed"
				return 1
			}
			zxfer_append_backup_metadata_record() {
				printf 'append %s %s %s\n' "$1" "$2" "$3" >>"$append_log"
			}
			zxfer_write_backup_properties() {
				printf 'unexpected write %s\n' "$g_backup_file_contents" >>"$append_log"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_option_k_backup_property_mode=1
			g_recursive_dest_list="backup/dst"
			g_actual_dest="backup/dst"
			zxfer_transfer_properties "tank/src"
		) 2>&1
	)
	status=$?
	append_log_contents=$(cat "$TEST_TMPDIR/transfer_failed_backup_capture.log")

	assertEquals "Property-transfer failures after source-property collection should still abort." 1 "$status"
	assertContains "Property-transfer failures before dataset completion should preserve the destination-property lookup failure." \
		"$output" "Failed to retrieve destination properties for [backup/dst]."
	assertEquals "Failed property transfers should not append or flush backup metadata before the dataset completes successfully." \
		"" "$append_log_contents"
}

test_transfer_properties_rethrows_override_derivation_failures() {
	set +e
	output=$(
		(
			call_log="$TEST_TMPDIR/transfer_override_failure.log"
			: >"$call_log"
			g_initial_source="tank/src"
			g_actual_dest="backup/dst"
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="compression=lz4=local"
				g_zxfer_source_pvs_effective="$g_zxfer_source_pvs_raw"
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			zxfer_validate_override_properties() {
				:
			}
			zxfer_ensure_required_properties_present() {
				printf '%s\n' "$2"
			}
			zxfer_derive_override_lists() {
				printf '%s\n' "derive failed" >&2
				return 7
			}
			zxfer_ensure_destination_exists() {
				printf 'ensure called\n' >>"$call_log"
				return 1
			}
			zxfer_collect_destination_props() {
				printf 'collect called\n' >>"$call_log"
				printf '%s\n' "compression=off=local"
			}
			zxfer_transfer_properties "tank/src"
		) 2>&1
	)
	status=$?

	assertEquals "Property transfer should abort when override derivation fails." 7 "$status"
	assertContains "Property transfer should surface the override-derivation failure instead of continuing with empty override/create lists." \
		"$output" "derive failed"
	assertEquals "Failed override derivation should prevent destination reconciliation from continuing." \
		"" "$(cat "$TEST_TMPDIR/transfer_override_failure.log")"
}

test_transfer_properties_selects_unsupported_properties_for_current_dataset_type() {
	log="$TEST_TMPDIR/transfer_unsupported_dataset_type.log"
	: >"$log"

	(
		LOG_FILE="$log"
		zxfer_collect_source_props() {
			g_zxfer_source_pvs_raw="volblocksize=16K=local"
			g_zxfer_source_pvs_effective="volblocksize=16K=local"
		}
		zxfer_run_source_zfs_cmd() {
			case "$4" in
			type)
				printf '%s\n' "volume"
				;;
			volsize)
				printf '%s\n' "1073741824"
				;;
			esac
		}
		zxfer_derive_override_lists() {
			g_zxfer_override_pvs_result="volblocksize=16K=local"
			g_zxfer_creation_pvs_result=""
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_strip_unsupported_properties() {
			printf 'strip %s || %s\n' "$1" "$2" >>"$LOG_FILE"
			printf '%s\n' "$1"
		}
		zxfer_ensure_required_properties_present() {
			printf '%s\n' "$2"
		}
		zxfer_ensure_destination_exists() {
			return 0
		}
		g_option_U_skip_unsupported_properties=1
		g_initial_source="tank/vol"
		g_actual_dest="backup/vol"
		g_zxfer_unsupported_filesystem_properties="compression"
		g_zxfer_unsupported_volume_properties="volblocksize"
		zxfer_transfer_properties "tank/vol"
	)

	assertEquals "Property transfer should pass the dataset-type-specific unsupported-property list into stripping." \
		"strip volblocksize=16K=local || volblocksize" "$(cat "$log")"
}

test_transfer_properties_diffs_existing_destinations_and_applies_changes() {
	log="$TEST_TMPDIR/transfer_existing.log"
	: >"$log"

	(
		LOG_FILE="$log"
		zxfer_collect_source_props() {
			g_zxfer_source_pvs_raw="compression=lz4=local"
			g_zxfer_source_pvs_effective="compression=lz4=local"
		}
		zxfer_run_source_zfs_cmd() {
			if [ "$4" = "type" ]; then
				printf '%s\n' "filesystem"
			else
				printf '%s\n' "-"
			fi
		}
		zxfer_derive_override_lists() {
			g_zxfer_override_pvs_result="compression=lz4=local"
			g_zxfer_creation_pvs_result=""
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_strip_unsupported_properties() {
			printf '%s\n' "$1"
		}
		zxfer_ensure_required_properties_present() {
			printf '%s\n' "$2"
		}
		zxfer_ensure_destination_exists() {
			return 1
		}
		zxfer_exists_destination() {
			printf '0\n'
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "compression=off=local"
		}
		zxfer_diff_properties() {
			printf 'compression=lz4\n'
			printf 'compression=lz4\n'
			printf '\n'
		}
		zxfer_apply_property_changes() {
			printf 'apply %s %s %s %s%s\n' "$1" "$2" "$3" "$4" "${5:+ $5}" >>"$LOG_FILE"
		}
		g_recursive_dest_list="backup/dst"
		g_actual_dest="backup/dst"
		zxfer_transfer_properties "tank/src/child"
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
		zxfer_collect_source_props() {
			g_zxfer_source_pvs_raw="compression=lz4=local"
			g_zxfer_source_pvs_effective="compression=lz4=local"
		}
		zxfer_run_source_zfs_cmd() {
			if [ "$4" = "type" ]; then
				printf '%s\n' "filesystem"
			else
				printf '%s\n' "-"
			fi
		}
		zxfer_ensure_required_properties_present() {
			printf 'ensure-required %s %s %s\n' "$1" "$2" "$4" >>"$LOG_FILE"
			case "$1" in
			tank/src) printf '%s\n' "compression=lz4=local,casesensitivity=sensitive=local" ;;
			backup/dst) printf '%s\n' "compression=off=local,casesensitivity=insensitive=local" ;;
			esac
		}
		zxfer_validate_override_properties() {
			:
		}
		zxfer_derive_override_lists() {
			g_zxfer_override_pvs_result="compression=lz4=local,casesensitivity=sensitive=local"
			g_zxfer_creation_pvs_result=""
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_strip_unsupported_properties() {
			printf '%s\n' "$1"
		}
		zxfer_ensure_destination_exists() {
			return 1
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "compression=off=local"
		}
		zxfer_diff_properties() {
			printf 'diff %s || %s || %s\n' "$1" "$2" "$3" >>"$LOG_FILE"
			printf '\n'
			printf '\n'
			printf '\n'
		}
		zxfer_apply_property_changes() {
			:
		}
		g_recursive_dest_list="backup/dst"
		g_actual_dest="backup/dst"
		zxfer_transfer_properties "tank/src"
	)

	result=$(cat "$log")
	assertContains "Source properties should be augmented with missing must-create entries before diffing." \
		"$result" "ensure-required tank/src compression=lz4=local casesensitivity,normalization,utf8only"
	assertContains "Destination properties should be augmented with missing must-create entries before diffing." \
		"$result" "ensure-required backup/dst compression=off=local casesensitivity,normalization,utf8only"
	assertContains "Property diffing should run after the must-create source properties are appended." \
		"$result" "compression=lz4=local,casesensitivity=sensitive=local"
	assertContains "Property diffing should run after the must-create destination properties are appended." \
		"$result" "compression=off=local,casesensitivity=insensitive=local"
	assertContains "Property diffing should still receive the must-create property list." \
		"$result" "casesensitivity,normalization,utf8only"
}

test_transfer_properties_propagates_must_create_diff_failures() {
	set +e
	output=$(
		(
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="compression=lz4=local"
				g_zxfer_source_pvs_effective="compression=lz4=local"
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			zxfer_validate_override_properties() {
				:
			}
			zxfer_derive_override_lists() {
				g_zxfer_override_pvs_result="compression=lz4=local"
				g_zxfer_creation_pvs_result=""
			}
			zxfer_sanitize_property_list() {
				printf '%s\n' "$1"
			}
			zxfer_strip_unsupported_properties() {
				printf '%s\n' "$1"
			}
			zxfer_ensure_required_properties_present() {
				printf '%s\n' "$2"
			}
			zxfer_ensure_destination_exists() {
				return 1
			}
			zxfer_collect_destination_props() {
				printf '%s\n' "compression=off=local"
			}
			zxfer_ensure_required_properties_present() {
				printf '%s\n' "$2"
			}
			zxfer_diff_properties() {
				zxfer_throw_error_with_usage "must-create mismatch"
			}
			zxfer_throw_error_with_usage() {
				printf '%s\n' "$1" >&2
				exit 2
			}
			g_recursive_dest_list="backup/dst"
			g_actual_dest="backup/dst"
			zxfer_transfer_properties "tank/src"
		) 2>&1
	)
	status=$?

	assertEquals "Must-create diff failures should propagate out of zxfer_transfer_properties." 2 "$status"
	assertContains "Must-create diff failures should preserve the diff error text." \
		"$output" "must-create mismatch"
}

test_transfer_properties_rethrows_tempfile_allocation_failures() {
	set +e
	output=$(
		(
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="compression=lz4=local"
				g_zxfer_source_pvs_effective="compression=lz4=local"
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_get_temp_file() {
				zxfer_throw_error "Error creating temporary file."
			}
			g_actual_dest="backup/dst"
			zxfer_transfer_properties "tank/src"
		) 2>&1
	)
	status=$?

	assertEquals "Property transfer should fail closed when one of its temp-file allocations fails." \
		1 "$status"
	assertEquals "Property transfer should preserve the temp-file allocation failure." \
		"Error creating temporary file." "$output"
}

test_transfer_properties_fails_when_source_required_property_probe_fails() {
	set +e
	output=$(
		(
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="compression=lz4=local"
				g_zxfer_source_pvs_effective="compression=lz4=local"
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			zxfer_run_zfs_cmd_for_spec() {
				if [ "$5" = "casesensitivity" ] && [ "$6" = "tank/src" ]; then
					printf '%s\n' "permission denied"
					return 1
				fi
				printf '%s\n' "unexpected probe $*"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_actual_dest="backup/dst"
			zxfer_transfer_properties "tank/src"
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
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="compression=lz4=local"
				g_zxfer_source_pvs_effective="compression=lz4=local"
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			zxfer_ensure_required_properties_present() {
				required_call_count=$((required_call_count + 1))
				if [ "$required_call_count" -eq 1 ]; then
					printf '%s\n' "$2"
					return 0
				fi
				printf '%s\n' "effective property probe failed"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_actual_dest="backup/dst"
			zxfer_transfer_properties "tank/src"
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
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="compression=lz4=local,casesensitivity=sensitive=local,normalization=none=local,jailed=off=local,utf8only=on=local"
				g_zxfer_source_pvs_effective="$g_zxfer_source_pvs_raw"
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			zxfer_run_zfs_cmd_for_spec() {
				if [ "$5" = "casesensitivity" ] && [ "$6" = "backup/dst" ]; then
					printf '%s\n' "ssh timeout"
					return 1
				fi
				printf '%s\n' "invalid property"
				return 1
			}
			zxfer_validate_override_properties() {
				:
			}
			zxfer_derive_override_lists() {
				g_zxfer_override_pvs_result="compression=lz4=local,casesensitivity=sensitive=local"
				g_zxfer_creation_pvs_result=""
			}
			zxfer_sanitize_property_list() {
				printf '%s\n' "$1"
			}
			zxfer_strip_unsupported_properties() {
				printf '%s\n' "$1"
			}
			zxfer_ensure_destination_exists() {
				return 1
			}
			zxfer_collect_destination_props() {
				printf '%s\n' "compression=off=local,normalization=none=local,jailed=off=local,utf8only=on=local"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_recursive_dest_list="backup/dst"
			g_actual_dest="backup/dst"
			zxfer_transfer_properties "tank/src"
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
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="compression=lz4=local"
				g_zxfer_source_pvs_effective="compression=lz4=local"
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			zxfer_ensure_required_properties_present() {
				printf '%s\n' "$2"
			}
			zxfer_validate_override_properties() {
				:
			}
			zxfer_derive_override_lists() {
				g_zxfer_override_pvs_result="compression=lz4=local"
				g_zxfer_creation_pvs_result=""
			}
			zxfer_sanitize_property_list() {
				printf '%s\n' "$1"
			}
			zxfer_strip_unsupported_properties() {
				printf '%s\n' "$1"
			}
			zxfer_ensure_destination_exists() {
				return 1
			}
			zxfer_collect_destination_props() {
				printf '%s\n' "ssh timeout"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			g_recursive_dest_list="backup/dst"
			g_actual_dest="backup/dst"
			zxfer_transfer_properties "tank/src"
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
		zxfer_collect_source_props() {
			g_zxfer_source_pvs_raw="compression=lz4=local"
			g_zxfer_source_pvs_effective="$g_zxfer_source_pvs_raw"
		}
		zxfer_run_source_zfs_cmd() {
			if [ "$4" = "type" ]; then
				printf '%s\n' "volume"
			elif [ "$4" = "volsize" ]; then
				printf '%s\n' "8M"
			else
				printf '%s\n' "-"
			fi
		}
		zxfer_ensure_required_properties_present() {
			printf 'ensure-required %s %s%s\n' "$1" "$2" "${4:+ $4}" >>"$LOG_FILE"
			if [ -n "$4" ]; then
				printf '%s\n' "unexpected required property list: $4"
				exit 1
			fi
			printf '%s\n' "$2"
		}
		zxfer_validate_override_properties() {
			:
		}
		zxfer_derive_override_lists() {
			g_zxfer_override_pvs_result="compression=lz4=local"
			g_zxfer_creation_pvs_result=""
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_strip_unsupported_properties() {
			printf '%s\n' "$1"
		}
		zxfer_ensure_destination_exists() {
			printf 'ensure %s %s %s\n' "$5" "$6" "$7" >>"$LOG_FILE"
			return 0
		}
		g_initial_source="tank/vol"
		g_actual_dest="backup/vol"
		zxfer_transfer_properties "tank/vol"
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
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="compression=lz4=local"
				g_zxfer_source_pvs_effective="$g_zxfer_source_pvs_raw"
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "permission denied"
					return 1
				fi
				printf '%s\n' "unexpected $*"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_transfer_properties "tank/src"
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
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="compression=lz4=local"
				g_zxfer_source_pvs_effective="$g_zxfer_source_pvs_raw"
			}
			zxfer_run_source_zfs_cmd() {
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
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_transfer_properties "tank/vol"
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
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="compression=lz4=local"
				g_zxfer_source_pvs_effective="$g_zxfer_source_pvs_raw"
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "volume"
				elif [ "$4" = "volsize" ]; then
					printf '\n'
				else
					printf '%s\n' "unexpected $*"
					return 1
				fi
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_transfer_properties "tank/vol"
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
	append_log="$TEST_TMPDIR/transfer_writable_backup.log"
	: >"$append_log"
	g_option_k_backup_property_mode=1
	g_ensure_writable=1
	g_option_o_override_property="readonly=on"
	g_initial_source="tank/src"
	g_actual_dest="backup/dst"
	g_recursive_dest_list=""
	zxfer_collect_source_props() {
		g_zxfer_source_pvs_raw="readonly=on=local,compression=lz4=local"
		g_zxfer_source_pvs_effective="readonly=off=local,compression=lz4=local"
	}
	zxfer_run_source_zfs_cmd() {
		if [ "$4" = "type" ]; then
			printf '%s\n' "filesystem"
		else
			printf '%s\n' "-"
		fi
	}
	zxfer_validate_override_properties() {
		printf 'validate %s\n' "$1" >>"$log"
	}
	zxfer_derive_override_lists() {
		g_zxfer_override_pvs_result="readonly=off=override,compression=lz4=local"
		g_zxfer_creation_pvs_result=""
	}
	zxfer_sanitize_property_list() {
		printf '%s\n' "$1"
	}
	zxfer_strip_unsupported_properties() {
		printf '%s\n' "$1"
	}
	zxfer_ensure_required_properties_present() {
		printf '%s\n' "$2"
	}
	zxfer_ensure_destination_exists() {
		return 0
	}
	zxfer_append_backup_metadata_record() {
		printf 'backup_append %s %s %s\n' "$1" "$2" "$3" >>"$append_log"
		g_backup_file_contents="helper-owned"
	}
	zxfer_write_backup_properties() {
		printf 'unexpected backup_write %s\n' "$g_backup_file_contents" >>"$append_log"
	}

	zxfer_transfer_properties "tank/src"

	unset -f zxfer_collect_source_props
	unset -f zxfer_run_source_zfs_cmd
	unset -f zxfer_validate_override_properties
	unset -f zxfer_derive_override_lists
	unset -f zxfer_sanitize_property_list
	unset -f zxfer_strip_unsupported_properties
	unset -f zxfer_ensure_destination_exists
	unset -f zxfer_append_backup_metadata_record
	unset -f zxfer_write_backup_properties

	assertContains "Writable-mode transfers should validate overrides after forcing readonly=off." \
		"$(cat "$log")" "validate readonly=off"
	assertContains "Writable-mode backup capture should preserve the raw source properties for restore mode through the owner helper." \
		"$(cat "$append_log")" "backup_append tank/src backup/dst readonly=on=local,compression=lz4=local"
	assertNotContains "Writable-mode property reconciliation should not flush backup metadata directly." \
		"$(cat "$append_log")" "unexpected backup_write"
	assertEquals "Writable-mode backup accumulation state should remain helper-owned." \
		"helper-owned" "$g_backup_file_contents"
	assertEquals "Created destinations should still be tracked in current-shell transfer tests." 1 "$g_dest_created_by_zxfer"
}

test_transfer_properties_preserves_escaped_comma_override_end_to_end() {
	log="$TEST_TMPDIR/transfer_escaped_override.log"
	: >"$log"

	(
		LOG_FILE="$log"
		g_initial_source="tank/src"
		g_actual_dest="backup/dst"
		g_recursive_dest_list="backup/dst"
		g_option_o_override_property='user:note=value\,with\,commas=and;semi'

		zxfer_collect_source_props() {
			g_zxfer_source_pvs_raw="user:note=existing=local"
			g_zxfer_source_pvs_effective="$g_zxfer_source_pvs_raw"
		}
		zxfer_run_source_zfs_cmd() {
			if [ "$4" = "type" ]; then
				printf '%s\n' "filesystem"
			else
				printf '%s\n' "-"
			fi
		}
		zxfer_validate_override_properties() {
			printf 'validate %s %s\n' "$1" "$2" >>"$LOG_FILE"
		}
		zxfer_derive_override_lists() {
			g_zxfer_override_pvs_result="user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=override"
			g_zxfer_creation_pvs_result=""
		}
		zxfer_ensure_required_properties_present() {
			printf '%s\n' "$2"
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_strip_unsupported_properties() {
			printf '%s\n' "$1"
		}
		zxfer_ensure_destination_exists() {
			return 1
		}
		zxfer_collect_destination_props() {
			printf '%s\n' "user:note=old=local"
		}
		zxfer_diff_properties() {
			printf '%s\n' "$1"
			printf '\n'
			printf '\n'
		}
		zxfer_apply_property_changes() {
			printf 'initial=%s\nset=%s\ninherit=%s\n' "$3" "$4" "$5" >>"$LOG_FILE"
		}
		zxfer_capture_backup_metadata_for_completed_transfer() {
			:
		}

		zxfer_transfer_properties "tank/src"
	)

	result=$(cat "$log")
	assertContains "Escaped-comma overrides should reach transfer-time validation without being split into fake assignments." \
		"$result" "validate user:note=value\,with\,commas=and;semi user:note=existing=local"
	assertContains "Escaped-comma overrides should survive the full property-transfer helper and reach the root-dataset apply path as one encoded assignment." \
		"$result" "initial=user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=override"
	assertContains "Root-dataset escaped-comma overrides should not be misrouted into child-set updates." \
		"$result" "set="
	assertContains "Root-dataset escaped-comma overrides should not manufacture inherit operations." \
		"$result" "inherit="
}

test_transfer_properties_prefers_forwarded_backup_provenance_for_chained_backup_capture() {
	append_log="$TEST_TMPDIR/transfer_forwarded_backup.log"
	: >"$append_log"
	g_option_k_backup_property_mode=1
	g_backup_file_extension=".zxfer_backup_info"
	g_initial_source="backup/intermediate/src"
	g_actual_dest="backup/final/src"
	g_recursive_dest_list=""
	zxfer_collect_source_props() {
		g_zxfer_source_pvs_raw="compression=off=local"
		g_zxfer_source_pvs_effective="$g_zxfer_source_pvs_raw"
	}
	zxfer_run_source_zfs_cmd() {
		if [ "$4" = "type" ]; then
			printf '%s\n' "filesystem"
		else
			printf '%s\n' "-"
		fi
	}
	zxfer_validate_override_properties() {
		:
	}
	zxfer_derive_override_lists() {
		g_zxfer_override_pvs_result="compression=off=local"
		g_zxfer_creation_pvs_result=""
	}
	zxfer_sanitize_property_list() {
		printf '%s\n' "$1"
	}
	zxfer_strip_unsupported_properties() {
		printf '%s\n' "$1"
	}
	zxfer_ensure_required_properties_present() {
		printf '%s\n' "$2"
	}
	zxfer_ensure_destination_exists() {
		return 0
	}
	zxfer_get_forwarded_backup_properties_for_source() {
		g_forwarded_backup_properties="compression=lz4=local"
		printf '%s\n' "$g_forwarded_backup_properties"
	}
	zxfer_append_backup_metadata_record() {
		printf 'backup_append %s %s %s\n' "$1" "$2" "$3" >>"$append_log"
	}

	zxfer_transfer_properties "backup/intermediate/src"

	unset -f zxfer_collect_source_props
	unset -f zxfer_run_source_zfs_cmd
	unset -f zxfer_validate_override_properties
	unset -f zxfer_derive_override_lists
	unset -f zxfer_sanitize_property_list
	unset -f zxfer_strip_unsupported_properties
	unset -f zxfer_ensure_required_properties_present
	unset -f zxfer_ensure_destination_exists
	unset -f zxfer_get_forwarded_backup_properties_for_source
	unset -f zxfer_append_backup_metadata_record

	assertContains "Chained backup capture should prefer forwarded provenance from the intermediate backup metadata over the intermediate dataset's live properties." \
		"$(cat "$append_log")" "backup_append backup/intermediate/src backup/final/src compression=lz4=local"
}

test_transfer_properties_skip_backup_capture_preserves_existing_backup_contents() {
	g_option_k_backup_property_mode=1
	g_backup_file_contents="existing"
	append_log="$TEST_TMPDIR/transfer_skip_backup.log"
	: >"$append_log"
	g_recursive_dest_list=""
	g_initial_source="tank/src"
	g_actual_dest="backup/dst"
	zxfer_collect_source_props() {
		g_zxfer_source_pvs_raw="readonly=on=local,compression=lz4=local"
		g_zxfer_source_pvs_effective="$g_zxfer_source_pvs_raw"
	}
	zxfer_run_source_zfs_cmd() {
		if [ "$4" = "type" ]; then
			printf '%s\n' "filesystem"
		else
			printf '%s\n' "-"
		fi
	}
	zxfer_validate_override_properties() {
		:
	}
	zxfer_derive_override_lists() {
		g_zxfer_override_pvs_result="readonly=on=local,compression=lz4=local"
		g_zxfer_creation_pvs_result=""
	}
	zxfer_sanitize_property_list() {
		printf '%s\n' "$1"
	}
	zxfer_strip_unsupported_properties() {
		printf '%s\n' "$1"
	}
	zxfer_ensure_required_properties_present() {
		printf '%s\n' "$2"
	}
	zxfer_ensure_destination_exists() {
		return 0
	}
	zxfer_append_backup_metadata_record() {
		printf 'unexpected %s %s %s\n' "$1" "$2" "$3" >>"$append_log"
	}

	zxfer_transfer_properties "tank/src" 1

	unset -f zxfer_collect_source_props
	unset -f zxfer_run_source_zfs_cmd
	unset -f zxfer_validate_override_properties
	unset -f zxfer_derive_override_lists
	unset -f zxfer_sanitize_property_list
	unset -f zxfer_strip_unsupported_properties
	unset -f zxfer_ensure_destination_exists
	unset -f zxfer_append_backup_metadata_record

	assertEquals "Post-seed reconciliation should not duplicate -k backup metadata." \
		"existing" "$g_backup_file_contents"
	assertEquals "Post-seed reconciliation should skip the backup append helper entirely." \
		"" "$(cat "$append_log")"
}

test_transfer_properties_adjusts_child_inherit_lists_for_existing_children() {
	log="$TEST_TMPDIR/transfer_child_adjust.log"
	: >"$log"
	(
		g_option_V_very_verbose=1
		g_initial_source="tank/src"
		g_actual_dest="backup/dst/child"
		g_recursive_dest_list="backup/dst
backup/dst/child"

		zxfer_collect_source_props() {
			g_zxfer_source_pvs_raw="compression=lz4=inherited"
			g_zxfer_source_pvs_effective="$g_zxfer_source_pvs_raw"
		}
		zxfer_run_source_zfs_cmd() {
			if [ "$4" = "type" ]; then
				printf '%s\n' "filesystem"
			else
				printf '%s\n' "-"
			fi
		}
		zxfer_ensure_required_properties_present() {
			printf '%s\n' "$2"
		}
		zxfer_derive_override_lists() {
			g_zxfer_override_pvs_result="compression=lz4=inherited"
			g_zxfer_creation_pvs_result=""
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_strip_unsupported_properties() {
			printf '%s\n' "$1"
		}
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_diff_properties() {
			printf '\n'
			printf '\n'
			printf 'compression=lz4\n'
		}
		zxfer_collect_destination_props() {
			case "$1" in
			backup/dst/child) printf '%s\n' "compression=lz4=local" ;;
			backup/dst) printf '%s\n' "compression=lz4=local" ;;
			*)
				printf '%s\n' "unexpected dataset $1"
				return 1
				;;
			esac
		}
		zxfer_apply_property_changes() {
			printf 'apply %s %s %s %s\n' "$1" "$2" "$3" "$4" >>"$log"
		}
		zxfer_ensure_destination_exists() {
			return 1
		}

		zxfer_transfer_properties "tank/src/child"
	) >"$log" 2>&1

	assertContains "Child transfers should reconcile inherit-vs-set state before applying destination property changes." \
		"$(cat "$log")" "zxfer_transfer_properties adjusted child_set:"
	assertContains "Child transfers should preserve the reconciled inherit list in very-verbose output." \
		"$(cat "$log")" "zxfer_transfer_properties adjusted inherit:"
}

test_transfer_properties_adjusts_set_only_inherited_child_properties_for_existing_children() {
	log="$TEST_TMPDIR/transfer_child_set_only_adjust.log"
	: >"$log"
	(
		g_option_V_very_verbose=1
		g_initial_source="tank/src"
		g_actual_dest="backup/dst/child"
		g_recursive_dest_list="backup/dst
backup/dst/child"

		zxfer_collect_source_props() {
			g_zxfer_source_pvs_raw="checksum=sha256=inherited"
			g_zxfer_source_pvs_effective="$g_zxfer_source_pvs_raw"
		}
		zxfer_run_source_zfs_cmd() {
			if [ "$4" = "type" ]; then
				printf '%s\n' "filesystem"
			else
				printf '%s\n' "-"
			fi
		}
		zxfer_ensure_required_properties_present() {
			printf '%s\n' "$2"
		}
		zxfer_derive_override_lists() {
			g_zxfer_override_pvs_result="checksum=sha256=inherited"
			g_zxfer_creation_pvs_result=""
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_strip_unsupported_properties() {
			printf '%s\n' "$1"
		}
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_diff_properties() {
			printf '\n'
			printf 'checksum=sha256\n'
			printf '\n'
		}
		zxfer_collect_destination_props() {
			case "$1" in
			backup/dst/child) printf '%s\n' "checksum=fletcher4=local" ;;
			backup/dst) printf '%s\n' "checksum=sha256=local" ;;
			*)
				printf '%s\n' "unexpected dataset $1"
				return 1
				;;
			esac
		}
		zxfer_apply_property_changes() {
			printf 'apply %s %s %s %s %s\n' "$1" "$2" "$3" "$4" "$5" >>"$log"
		}
		zxfer_ensure_destination_exists() {
			return 1
		}

		zxfer_transfer_properties "tank/src/child"
	) >"$log" 2>&1

	assertContains "Child transfers should still reconcile inherited-source properties that initially appear only in the set list." \
		"$(cat "$log")" "zxfer_transfer_properties adjusted child_set: "
	assertContains "Set-only inherited-source properties should be demoted back into the inherit list when the parent already matches." \
		"$(cat "$log")" "zxfer_transfer_properties adjusted inherit: checksum=sha256"
	assertContains "The final property application should inherit the reconciled property instead of setting it locally." \
		"$(cat "$log")" "apply backup/dst/child 0   checksum=sha256"
}

test_transfer_properties_uses_freebsd_readonly_properties_without_mutating_global_state() {
	log="$TEST_TMPDIR/transfer_freebsd_readonly.log"
	: >"$log"
	g_destination_operating_system="FreeBSD"
	g_source_operating_system="Linux"
	g_test_base_readonly_properties="readonly"
	g_test_freebsd_readonly_properties="aclmode"
	g_initial_source="tank/src"
	g_actual_dest="backup/dst"
	g_recursive_dest_list=""
	zxfer_collect_source_props() {
		g_zxfer_source_pvs_raw="compression=lz4=local"
		g_zxfer_source_pvs_effective="compression=lz4=local"
	}
	zxfer_run_source_zfs_cmd() {
		if [ "$4" = "type" ]; then
			printf '%s\n' "filesystem"
		else
			printf '%s\n' "-"
		fi
	}
	zxfer_validate_override_properties() {
		:
	}
	zxfer_derive_override_lists() {
		g_zxfer_override_pvs_result=""
		g_zxfer_creation_pvs_result=""
	}
	zxfer_sanitize_property_list() {
		printf '%s\n' "$2" >>"$log"
		printf '%s\n' "$1"
	}
	zxfer_strip_unsupported_properties() {
		printf '%s\n' "$1"
	}
	zxfer_ensure_required_properties_present() {
		printf '%s\n' "$2"
	}
	zxfer_ensure_destination_exists() {
		return 0
	}

	zxfer_transfer_properties "tank/src"
	zxfer_transfer_properties "tank/src"

	unset -f zxfer_collect_source_props
	unset -f zxfer_run_source_zfs_cmd
	unset -f zxfer_validate_override_properties
	unset -f zxfer_derive_override_lists
	unset -f zxfer_sanitize_property_list
	unset -f zxfer_strip_unsupported_properties
	unset -f zxfer_ensure_destination_exists

	assertEquals "FreeBSD-specific readonly properties should be applied per transfer without mutating the global base list." \
		"readonly" "$(zxfer_get_base_readonly_properties)"
	assertEquals "Repeated transfers should reuse the same effective FreeBSD readonly list instead of appending duplicates." \
		"readonly,aclmode
readonly,aclmode" "$(cat "$log")"
}

test_transfer_properties_uses_solexp_readonly_properties_without_mutating_global_state() {
	log="$TEST_TMPDIR/transfer_solexp_readonly.log"
	: >"$log"
	g_destination_operating_system="SunOS"
	g_source_operating_system="FreeBSD"
	g_test_base_readonly_properties="readonly"
	g_test_solexp_readonly_properties="jailed"
	g_initial_source="tank/src"
	g_actual_dest="backup/dst"
	g_recursive_dest_list=""
	zxfer_collect_source_props() {
		g_zxfer_source_pvs_raw="compression=lz4=local"
		g_zxfer_source_pvs_effective="compression=lz4=local"
	}
	zxfer_run_source_zfs_cmd() {
		if [ "$4" = "type" ]; then
			printf '%s\n' "filesystem"
		else
			printf '%s\n' "-"
		fi
	}
	zxfer_validate_override_properties() {
		:
	}
	zxfer_derive_override_lists() {
		g_zxfer_override_pvs_result=""
		g_zxfer_creation_pvs_result=""
	}
	zxfer_sanitize_property_list() {
		printf '%s\n' "$2" >>"$log"
		printf '%s\n' "$1"
	}
	zxfer_strip_unsupported_properties() {
		printf '%s\n' "$1"
	}
	zxfer_ensure_required_properties_present() {
		printf '%s\n' "$2"
	}
	zxfer_ensure_destination_exists() {
		return 0
	}

	zxfer_transfer_properties "tank/src"
	zxfer_transfer_properties "tank/src"

	unset -f zxfer_collect_source_props
	unset -f zxfer_run_source_zfs_cmd
	unset -f zxfer_validate_override_properties
	unset -f zxfer_derive_override_lists
	unset -f zxfer_sanitize_property_list
	unset -f zxfer_strip_unsupported_properties
	unset -f zxfer_ensure_destination_exists

	assertEquals "SunOS-specific readonly properties should be applied per transfer without mutating the global base list." \
		"readonly" "$(zxfer_get_base_readonly_properties)"
	assertEquals "Repeated FreeBSD-to-SunOS transfers should reuse the same effective readonly list instead of appending duplicates." \
		"readonly,jailed
readonly,jailed" "$(cat "$log")"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
