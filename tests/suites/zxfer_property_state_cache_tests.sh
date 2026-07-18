#!/bin/sh
# Property state, cache, normalization, and recursive-prefetch behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_readonly_property_constants_pin_source_lists() {
	result=$(
		(
			# shellcheck source=src/zxfer_property_policy.sh
			. "$TESTS_DIR/../src/zxfer_property_policy.sh"
			printf 'base=%s\n' "$ZXFER_BASE_READONLY_PROPERTIES"
			printf 'freebsd=%s\n' "$ZXFER_FREEBSD_READONLY_PROPERTIES"
		) 2>&1
	)

	assertContains "The base readonly getter should return the source-time constant list." \
		"$result" "base=type,creation,used,available"
	assertContains "The FreeBSD readonly getter should return the platform constant list." \
		"$result" "freebsd=aclmode,aclinherit,devices,nbmand,shareiscsi,vscan,xattr,dnodesize"
	assertNotContains "The readonly profile stack should no longer expose the retired Solaris Express compatibility list." \
		"$result" "solexp="
}

test_get_effective_readonly_properties_removes_mountpoint_during_migration() {
	result=$(
		(
			g_option_m_migrate=1
			g_destination_operating_system="FreeBSD"
			# shellcheck source=src/zxfer_property_policy.sh
			. "$TESTS_DIR/../src/zxfer_property_policy.sh"
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

test_get_effective_readonly_properties_removes_mountpoint_in_current_shell() {
	old_migrate_flag=${g_option_m_migrate:-0}
	g_option_m_migrate=1

	result=$(zxfer_get_effective_readonly_properties)
	g_option_m_migrate=$old_migrate_flag

	assertEquals "Migration should remove mountpoint from the effective readonly list even when the helper runs in the current shell." \
		"readonly" "$result"
}

test_get_effective_readonly_properties_uses_freebsd_list_when_base_is_empty() {
	result=$(
		(
			g_destination_operating_system="FreeBSD"
			# shellcheck source=src/zxfer_property_policy.sh
			. "$TESTS_DIR/../src/zxfer_property_policy.sh"
			ZXFER_BASE_READONLY_PROPERTIES=""
			zxfer_get_effective_readonly_properties
		)
	)

	assertEquals "An empty base list should still allow the FreeBSD readonly list to become the full effective list." \
		"aclmode,aclinherit,devices,nbmand,shareiscsi,vscan,xattr,dnodesize" "$result"
}

test_get_effective_readonly_properties_uses_base_list_for_sunos_without_platform_delta_when_base_is_empty() {
	result=$(
		(
			g_destination_operating_system="SunOS"
			g_source_operating_system="FreeBSD"
			# shellcheck source=src/zxfer_property_policy.sh
			. "$TESTS_DIR/../src/zxfer_property_policy.sh"
			ZXFER_BASE_READONLY_PROPERTIES=""
			zxfer_get_effective_readonly_properties
		)
	)

	assertEquals "SunOS destinations should not add an extra readonly-property delta on the OpenZFS 2+ floor." \
		"" "$result"
}

test_zxfer_property_owner_operations_and_state_helpers_cover_current_shell_paths() {
	stage_file="$TEST_TMPDIR/property_reconcile_stage_file.out"
	stage_read_output_file="$TEST_TMPDIR/property_reconcile_stage_read.out"
	adjusted_output_file="$TEST_TMPDIR/property_adjusted_publish.out"
	printf '%s\n' "compression=lz4=local" >"$stage_file"

	zxfer_append_unsupported_property_for_dataset_type filesystem compression
	zxfer_append_unsupported_property_for_dataset_type volume volblocksize
	zxfer_reset_property_runtime_state

	zxfer_publish_remove_sources_result "stale-remove-sources"
	zxfer_publish_remove_properties_result "stale-remove"
	zxfer_publish_supported_properties_result "stale-supported"
	zxfer_publish_adjusted_property_lists "stale-set" "stale-inherit" >"$adjusted_output_file"
	zxfer_publish_source_property_results "stale-raw" "stale-effective"
	zxfer_publish_override_property_results "stale-override" "stale-create"
	zxfer_publish_required_property_backfill_result "stale-backfill"
	zxfer_set_property_transfer_initial_source 1
	zxfer_publish_property_transfer_source_metadata volume 1G volblocksize
	zxfer_publish_property_transfer_source_properties "compression=lz4=local"
	zxfer_publish_property_transfer_override_results "readonly=off=override" "volblocksize=8K=local"
	zxfer_publish_property_transfer_destination_properties "compression=off=local"
	zxfer_publish_property_transfer_diff_results "compression=lz4" "readonly=off" compression
	adjusted_output=$(cat "$adjusted_output_file")

	assertEquals "The adjusted-list owner operation should preserve its two-line stdout contract." \
		"stale-set
stale-inherit" "$adjusted_output"
	assertEquals "The remove-sources owner operation should publish its state result." \
		"stale-remove-sources" "$g_zxfer_new_rmvs_pv"
	assertEquals "The remove-properties owner operation should publish its state result." \
		"stale-remove" "$g_zxfer_new_rmv_pvs"
	assertEquals "The supported-properties owner operation should publish its state result." \
		"stale-supported" "$g_zxfer_only_supported_properties"
	assertEquals "The source-property owner operation should publish raw and effective results together." \
		"stale-raw|stale-effective" "$g_zxfer_source_pvs_raw|$g_zxfer_source_pvs_effective"
	assertEquals "The override-property owner operation should publish override and creation results together." \
		"stale-override|stale-create" "$g_zxfer_override_pvs_result|$g_zxfer_creation_pvs_result"
	assertEquals "The required-property owner operation should publish its backfill result." \
		"stale-backfill" "$g_zxfer_required_property_backfill_result"
	assertEquals "The transfer metadata owner operations should publish the complete source context." \
		"1|volume|1G|volblocksize|compression=lz4=local" \
		"$g_zxfer_property_transfer_is_initial_source|$g_zxfer_property_transfer_source_dstype|$g_zxfer_property_transfer_source_volsize|$g_zxfer_property_transfer_must_create_properties|$g_zxfer_property_transfer_source_pvs"
	assertEquals "The transfer plan owner operations should publish override, creation, destination, and diff state." \
		"readonly=off=override|volblocksize=8K=local|compression=off=local|compression=lz4|readonly=off|compression" \
		"$g_zxfer_property_transfer_override_pvs|$g_zxfer_property_transfer_creation_pvs|$g_zxfer_property_transfer_dest_pvs|$g_zxfer_property_transfer_initial_set_list|$g_zxfer_property_transfer_child_set_list|$g_zxfer_property_transfer_inherit_list"

	g_zxfer_property_stage_file_read_result="stale-stage"
	zxfer_reset_property_reconcile_state
	zxfer_read_property_reconcile_stage_file "$stage_file" >"$stage_read_output_file"
	read_output=$(cat "$stage_read_output_file")
	property_multiwriters=$(
		for l_owner_module in "$ZXFER_ROOT"/src/*.sh; do
			awk -v module="$l_owner_module" -f "$ZXFER_ROOT/tests/extract_global_writes.awk" "$l_owner_module"
		done | awk -F '	' '
		{
			writer_key = $1 SUBSEP $2
			if (!seen_writer[writer_key]++)
				writer_count[$1]++
			if ($2 ~ /\/src\/zxfer_property_(state|policy|reconcile)[.]sh$/)
				property_variable[$1] = 1
		}
		END {
			for (variable in property_variable) {
				if (writer_count[variable] > 1)
					print variable
			}
		}'
	)

	assertEquals "Resetting property runtime state should clear the filesystem unsupported-property cache." \
		"" "$g_zxfer_unsupported_filesystem_properties"
	assertEquals "Resetting property runtime state should clear the volume unsupported-property cache." \
		"" "$g_zxfer_unsupported_volume_properties"
	assertEquals "Resetting property reconcile state should clear the remove-sources scratch list." \
		"" "$g_zxfer_new_rmvs_pv"
	assertEquals "Resetting property reconcile state should clear the remove-properties scratch list." \
		"" "$g_zxfer_new_rmv_pvs"
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
	assertEquals "Resetting property reconcile state should clear the required-property backfill result." \
		"" "$g_zxfer_required_property_backfill_result"
	assertEquals "Resetting property reconcile state should clear every transfer-context result." \
		"0||||||||||" \
		"$g_zxfer_property_transfer_is_initial_source|$g_zxfer_property_transfer_source_dstype|$g_zxfer_property_transfer_source_volsize|$g_zxfer_property_transfer_must_create_properties|$g_zxfer_property_transfer_source_pvs|$g_zxfer_property_transfer_override_pvs|$g_zxfer_property_transfer_creation_pvs|$g_zxfer_property_transfer_dest_pvs|$g_zxfer_property_transfer_initial_set_list|$g_zxfer_property_transfer_child_set_list|$g_zxfer_property_transfer_inherit_list"
	assertEquals "Property reconcile stage-file reads should trim the trailing newline from staged output." \
		"compression=lz4=local" "$read_output"
	assertEquals "Property reconcile stage-file reads should publish the staged payload in shared scratch state." \
		"compression=lz4=local" "$g_zxfer_property_stage_file_read_result"
	assertEquals "Property result globals should have exactly one production owner module." \
		"" "$property_multiwriters"
}

test_zxfer_read_property_reconcile_stage_file_rethrows_runtime_artifact_read_failures_in_current_shell() {
	output=$(
		(
			set +e
			g_zxfer_property_stage_file_read_result="stale-stage"
			zxfer_read_runtime_artifact_file() {
				g_zxfer_runtime_artifact_read_result="partial-stage"
				return 23
			}
			zxfer_read_property_reconcile_stage_file "$TEST_TMPDIR/missing-stage" >/dev/null
			printf 'status=%s\n' "$?"
			printf 'result=<%s>\n' "${g_zxfer_property_stage_file_read_result:-}"
		)
	)

	assertContains "Property reconcile stage-file reads should preserve runtime-artifact read failures." \
		"$output" "status=23"
	assertContains "Property reconcile stage-file reads should clear stale published stage data before a failed readback." \
		"$output" "result=<>"
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

	assertContains "Effective readonly-property resolution should append the FreeBSD platform list to the base list when the base list is already populated." \
		"$freebsd_result" "readonly,mountpoint,aclmode"
	assertEquals "SunOS destinations should use the shared OpenZFS 2+ readonly-property list when the base list is populated." \
		"readonly,mountpoint" "$sunos_result"
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
			selected=$(zxfer_select_unsupported_properties_for_dataset_type filesystem)
			printf 'destination=%s\n' "$probe_destination"
			printf 'dataset=%s\n' "$probe_dataset"
			printf 'type=%s\n' "$probe_type"
			printf 'filesystem=%s\n' "$g_zxfer_unsupported_filesystem_properties"
			printf 'volume=%s\n' "$g_zxfer_unsupported_volume_properties"
			printf 'selected=%s\n' "$selected"
		) 2>&1
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

test_remove_properties_preserves_override_entries() {
	l_oldifs=$IFS
	IFS=","
	zxfer_remove_properties "mountpoint=/mnt=local,readonly=off=override,compression=lz4=local" "readonly,mountpoint"
	IFS=$l_oldifs

	assertEquals "Override entries should survive property filtering even when the property is listed for removal." \
		"readonly=off=override,compression=lz4=local" "$g_zxfer_new_rmv_pvs"
}

test_remove_properties_trims_remaining_filter_list_with_literal_property_names() {
	l_oldifs=$IFS
	IFS=","
	zxfer_remove_properties "user:a.b=one=local,user:axb=two=local,compression=lz4=local" "user:a.b,user:axb"
	IFS=$l_oldifs

	assertEquals "Property removal should not treat property names as regular expressions while trimming matched filters." \
		"compression=lz4=local" "$g_zxfer_new_rmv_pvs"
}

test_run_zfs_create_with_properties_executes_live_create() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$*"
			}
			zxfer_run_zfs_create_with_properties no volume 10G "compression=lz4,atime=off" "backup/dst"
		)
	)

	assertEquals "Live zfs create should pass each argument separately." \
		"create -V 10G -o compression=lz4 -o atime=off backup/dst" "$result"
}

test_run_zfs_create_with_properties_allows_parent_create_without_properties() {
	result=$(
		(
			zxfer_run_destination_zfs_cmd() {
				printf '%s\n' "$*"
			}
			zxfer_run_zfs_create_with_properties yes filesystem "" "" "backup/dst"
		)
	)

	assertEquals "Parent hierarchy creates without properties should still use -p." \
		"create -p backup/dst" "$result"
}

test_run_zfs_create_with_properties_rejects_parent_create_with_properties() {
	set +e
	output=$(zxfer_run_zfs_create_with_properties yes filesystem "" "compression=lz4" "backup/dst" 2>&1)
	status=$?
	set -e

	assertEquals "Parent hierarchy creates must fail closed instead of rendering zfs create -p with -o properties." \
		1 "$status"
	assertEquals "Rejected unsafe parent-property creates should not render a partial command." "" "$output"
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

test_get_normalized_dataset_properties_escapes_line_feed_values() {
	result=$(
		(
			zxfer_run_zfs_cmd_for_spec() {
				printf 'user:note\tline1\nline2\tlocal\n'
			}
			zxfer_get_normalized_dataset_properties "tank/src" "/sbin/zfs"
		)
	)

	assertEquals "Normalized property lookup should encode embedded line feeds instead of dropping the property." \
		"user:note=line1%0Aline2=local" "$result"
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
	table_hit=0
	if zxfer_property_table_find_dataset source "tank/src"; then
		table_hit=1
	fi

	unset -f zxfer_run_zfs_cmd_for_spec

	assertEquals "Failed normalized-property reads should return a non-zero status." 1 "$status"
	assertEquals "Failed normalized-property reads should surface the underlying zfs error." \
		"permission denied" "$(cat "$err_log")"
	assertEquals "A later successful lookup for the same dataset should still execute the full normalized read instead of reusing a poisoned table row." \
		"compression=lz4=local" "$(cat "$out_log")"
	assertEquals "A failed normalized lookup should not create a property table row before the later successful read stores one." \
		"3" "$(awk 'END {print NR + 0}' "$calls_log")"
	assertEquals "Successful normalized lookups after a failure should still populate the in-memory property table." \
		"1" "$table_hit"
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
	table_hit=0
	if zxfer_property_table_find_dataset source "tank/src"; then
		table_hit=1
	fi

	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f zxfer_serialize_property_records_from_stdin
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

	assertNotEquals "Machine-side serializer failures should return a non-zero status." \
		"0" "$status"
	assertEquals "Machine-side serializer failures should surface the serializer stderr." \
		"machine serializer failed" "$(cat "$err_log")"
	assertEquals "Machine-side serializer failures should stop before the human probe runs." \
		"1" "$(awk 'END {print NR + 0}' "$calls_log")"
	assertEquals "Machine-side serializer failures should not populate the in-memory property table." \
		"0" "$table_hit"
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
	table_hit=0
	if zxfer_property_table_find_dataset source "tank/src"; then
		table_hit=1
	fi

	unset -f zxfer_get_temp_file
	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f cat
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

	assertEquals "Machine-side serializer readback failures should return the staged readback status." \
		"26" "$status"
	assertEquals "Machine-side serializer readback failures should surface the staged readback diagnostic." \
		"machine serializer readback failed" "$(cat "$err_log")"
	assertEquals "Machine-side serializer readback failures should stop before the human probe runs." \
		"1" "$(awk 'END {print NR + 0}' "$calls_log")"
	assertEquals "Machine-side serializer readback failures should not populate the in-memory property table." \
		"0" "$table_hit"
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
	table_hit=0
	if zxfer_property_table_find_dataset source "tank/src"; then
		table_hit=1
	fi

	unset -f zxfer_run_zfs_cmd_for_spec

	assertEquals "Human normalized-property probe failures should return a non-zero status." \
		"1" "$status"
	assertEquals "Human normalized-property probe failures should surface the underlying zfs error." \
		"ssh timeout" "$(cat "$err_log")"
	assertEquals "Human normalized-property probe failures should still execute both normalized probes before failing." \
		"2" "$(awk 'END {print NR + 0}' "$calls_log")"
	assertEquals "Human normalized-property probe failures should not populate the in-memory property table." \
		"0" "$table_hit"
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
	table_hit=0
	if zxfer_property_table_find_dataset source "tank/src"; then
		table_hit=1
	fi

	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f zxfer_serialize_property_records_from_stdin
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

	assertNotEquals "Human-side serializer failures should return a non-zero status." \
		"0" "$status"
	assertEquals "Human-side serializer failures should surface the serializer stderr." \
		"human serializer failed" "$(cat "$err_log")"
	assertEquals "Human-side serializer failures should still execute both normalized-property probes." \
		"2" "$(awk 'END {print NR + 0}' "$calls_log")"
	assertEquals "Human-side serializer failures should not populate the in-memory property table." \
		"0" "$table_hit"
}

test_load_normalized_dataset_properties_uses_prefetched_table_row_when_available() {
	calls_log="$TEST_TMPDIR/normalized_prefetch_table.calls"
	: >"$calls_log"
	output=$(
		(
			CALLS_LOG="$calls_log"
			zxfer_maybe_prefetch_recursive_normalized_properties() {
				zxfer_property_table_append_dataset "$3" "$1" "compression=lz4=local"
				zxfer_property_table_find_dataset "$3" "$1"
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

	assertEquals "Normalized-property loads should reuse a prefetched table row when recursive prefetch materializes the dataset." \
		"props=compression=lz4=local
cache_hit=1
calls=0" "$output"
}

test_load_normalized_dataset_properties_falls_back_to_live_probe_when_prefetch_does_not_materialize_row() {
	calls_log="$TEST_TMPDIR/normalized_prefetch_empty_table.calls"
	: >"$calls_log"

	output=$(
		(
			CALLS_LOG="$calls_log"
			zxfer_maybe_prefetch_recursive_normalized_properties() {
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

	assertEquals "Prefetch passes that do not materialize the requested dataset's table row should be treated as misses and retried live." \
		"props=compression=lz4=local
cache_hit=0
calls=2" "$output"
}

test_zxfer_property_table_round_trips_hostile_dataset_names() {
	l_dataset="../unsafe path:/child"

	zxfer_property_table_append_dataset destination "$l_dataset" "user:note=line1%0Aline2=local"
	zxfer_property_table_append_dataset destination "backup/other" "compression=lz4=local"

	found=0
	if zxfer_property_table_find_dataset destination "$l_dataset"; then
		found=1
	fi
	payload=$g_zxfer_property_table_lookup_result

	zxfer_property_table_invalidate_dataset destination "$l_dataset" 0

	stale=0
	if zxfer_property_table_find_dataset destination "$l_dataset"; then
		stale=1
	fi
	survivor=0
	if zxfer_property_table_find_dataset destination "backup/other"; then
		survivor=1
	fi

	assertEquals "Hostile dataset names should round-trip through the in-memory property table." \
		"1" "$found"
	assertEquals "Hostile dataset rows should preserve their encoded property payload exactly." \
		"user:note=line1%0Aline2=local" "$payload"
	assertEquals "Destination table invalidation should remove rows keyed by hostile dataset names." \
		"0" "$stale"
	assertEquals "Destination table invalidation must not remove unrelated rows when stripping hostile dataset names." \
		"1" "$survivor"
}

test_zxfer_invalidate_destination_property_mutation_cache_strips_descendants_and_keeps_siblings() {
	zxfer_property_table_append_dataset destination "backup/dst" "compression=lz4=local"
	zxfer_property_table_append_dataset destination "backup/dst/child" "compression=gzip=inherited"
	zxfer_property_table_append_dataset destination "backup/dst2" "atime=off=local"
	zxfer_property_table_append_dataset destination "backup/dst/child" "casesensitivity=sensitive=local" "casesensitivity"
	zxfer_property_table_append_dataset destination "backup/dst2" "casesensitivity=sensitive=local" "casesensitivity"
	g_zxfer_destination_property_tree_prefetch_state=1

	zxfer_invalidate_destination_property_mutation_cache "backup/dst"

	mutated=0
	if zxfer_property_table_find_dataset destination "backup/dst"; then
		mutated=1
	fi
	child=0
	if zxfer_property_table_find_dataset destination "backup/dst/child"; then
		child=1
	fi
	sibling=0
	if zxfer_property_table_find_dataset destination "backup/dst2"; then
		sibling=1
	fi
	child_required=0
	if zxfer_property_table_find_dataset destination "backup/dst/child" "casesensitivity"; then
		child_required=1
	fi
	sibling_required=0
	if zxfer_property_table_find_dataset destination "backup/dst2" "casesensitivity"; then
		sibling_required=1
	fi

	assertEquals "A destination mutation should invalidate the mutated dataset's normalized table row." \
		"0" "$mutated"
	assertEquals "A destination mutation should invalidate descendant normalized table rows because inherited values may have changed." \
		"0" "$child"
	assertEquals "A destination mutation must not invalidate unrelated sibling datasets' normalized table rows." \
		"1" "$sibling"
	assertEquals "A destination mutation should invalidate descendant required-property probe rows." \
		"0" "$child_required"
	assertEquals "A destination mutation must not invalidate unrelated sibling required-property probe rows." \
		"1" "$sibling_required"
	assertEquals "Targeted destination mutation invalidation must keep the prefetched destination tree warm for unaffected datasets." \
		"1" "${g_zxfer_destination_property_tree_prefetch_state:-0}"
}

test_zxfer_invalidate_destination_property_mutation_cache_without_dataset_resets_destination_tables() {
	zxfer_property_table_append_dataset destination "backup/dst" "compression=lz4=local"
	zxfer_property_table_append_dataset source "tank/src" "compression=lz4=local"
	g_zxfer_destination_property_tree_prefetch_state=1

	zxfer_invalidate_destination_property_mutation_cache ""

	destination_row=0
	if zxfer_property_table_find_dataset destination "backup/dst"; then
		destination_row=1
	fi
	source_row=0
	if zxfer_property_table_find_dataset source "tank/src"; then
		source_row=1
	fi

	assertEquals "Mutation invalidation without a dataset should fall back to the full destination table reset." \
		"0" "$destination_row"
	assertEquals "The full destination table reset should preserve source table rows." \
		"1" "$source_row"
	assertEquals "The full destination table reset should rearm destination property-tree prefetch." \
		"0" "${g_zxfer_destination_property_tree_prefetch_state:-1}"
}

test_zxfer_reset_property_iteration_caches_clears_tables_memo_and_prefetch_state() {
	zxfer_property_table_append_dataset source "tank/src" "compression=lz4=local"
	zxfer_property_table_append_dataset destination "backup/dst" "compression=lz4=local"
	zxfer_property_table_append_dataset source "tank/src" "casesensitivity=sensitive=local" "casesensitivity"
	zxfer_property_table_append_dataset destination "backup/dst" "casesensitivity=sensitive=local" "casesensitivity"
	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/source/zfs"
	g_zxfer_source_property_tree_prefetch_state=1
	g_zxfer_destination_property_tree_prefetch_root="backup/dst"
	g_zxfer_destination_property_tree_prefetch_zfs_cmd="/dest/zfs"
	g_zxfer_destination_property_tree_prefetch_state=1
	g_zxfer_property_table_lookup_result="stale-lookup"

	zxfer_reset_property_iteration_caches

	assertEquals "Resetting property iteration caches should clear the source normalized table." \
		"" "${g_zxfer_source_property_table:-}"
	assertEquals "Resetting property iteration caches should clear the destination normalized table." \
		"" "${g_zxfer_destination_property_table:-}"
	assertEquals "Resetting property iteration caches should clear the source required-property table." \
		"" "${g_zxfer_source_required_property_table:-}"
	assertEquals "Resetting property iteration caches should clear the destination required-property table." \
		"" "${g_zxfer_destination_required_property_table:-}"
	assertEquals "Resetting property iteration caches should clear the last-dataset memo." \
		"" "${g_zxfer_property_table_memo_dataset:-}"
	assertEquals "Resetting property iteration caches should clear the source recursive property-tree root." \
		"" "${g_zxfer_source_property_tree_prefetch_root:-}"
	assertEquals "Resetting property iteration caches should clear the destination recursive property-tree root." \
		"" "${g_zxfer_destination_property_tree_prefetch_root:-}"
	assertEquals "Resetting property iteration caches should reset the source recursive property-tree state." \
		"0" "${g_zxfer_source_property_tree_prefetch_state:-1}"
	assertEquals "Resetting property iteration caches should reset the destination recursive property-tree state." \
		"0" "${g_zxfer_destination_property_tree_prefetch_state:-1}"
	assertEquals "Resetting property iteration caches should clear table lookup scratch state." \
		"" "${g_zxfer_property_table_lookup_result:-}"
}

test_zxfer_property_table_invalidate_dataset_removes_exact_source_rows_only() {
	zxfer_property_table_append_dataset source "tank/src" "compression=lz4=local"
	zxfer_property_table_append_dataset source "tank/src/child" "compression=gzip=inherited"
	zxfer_property_table_append_dataset source "tank/src" "casesensitivity=sensitive=local" "casesensitivity"

	zxfer_property_table_invalidate_dataset source "tank/src" 0

	exact=0
	if zxfer_property_table_find_dataset source "tank/src"; then
		exact=1
	fi
	child=0
	if zxfer_property_table_find_dataset source "tank/src/child"; then
		child=1
	fi
	required=0
	if zxfer_property_table_find_dataset source "tank/src" "casesensitivity"; then
		required=1
	fi

	assertEquals "Generic dataset invalidation should remove the exact source normalized table row." \
		"0" "$exact"
	assertEquals "Generic dataset invalidation should keep descendant rows when no descendant scope was requested." \
		"1" "$child"
	assertEquals "Generic dataset invalidation should remove the dataset's required-property probe rows." \
		"0" "$required"
}

test_zxfer_property_table_invalidate_dataset_ignores_unknown_sides() {
	zxfer_property_table_append_dataset source "tank/src" "compression=lz4=local"

	zxfer_property_table_invalidate_dataset unknown "tank/src" 1

	source_row=0
	if zxfer_property_table_find_dataset source "tank/src"; then
		source_row=1
	fi

	assertEquals "Unknown-side invalidation should be a no-op so callers can share cleanup paths safely." \
		"1" "$source_row"
}

test_zxfer_reset_destination_property_iteration_cache_preserves_source_table_rows() {
	zxfer_property_table_append_dataset source "tank/src" "compression=lz4=local"
	zxfer_property_table_append_dataset source "tank/src" "casesensitivity=sensitive=local" "casesensitivity"
	zxfer_property_table_append_dataset destination "backup/dst" "compression=lz4=local"
	zxfer_property_table_append_dataset destination "backup/dst" "casesensitivity=sensitive=local" "casesensitivity"

	zxfer_reset_destination_property_iteration_cache

	source_row=0
	if zxfer_property_table_find_dataset source "tank/src"; then
		source_row=1
	fi
	source_required=0
	if zxfer_property_table_find_dataset source "tank/src" "casesensitivity"; then
		source_required=1
	fi
	destination_row=0
	if zxfer_property_table_find_dataset destination "backup/dst"; then
		destination_row=1
	fi
	destination_required=0
	if zxfer_property_table_find_dataset destination "backup/dst" "casesensitivity"; then
		destination_required=1
	fi

	assertEquals "Destination-table resets should preserve source normalized table rows." \
		"1" "$source_row"
	assertEquals "Destination-table resets should preserve source required-property probe rows." \
		"1" "$source_required"
	assertEquals "Destination-table resets should clear destination normalized table rows." \
		"0" "$destination_row"
	assertEquals "Destination-table resets should clear destination required-property probe rows." \
		"0" "$destination_required"
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
		"$ZXFER_BASE_READONLY_PROPERTIES" >"$outfile"

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
	first_stage_file="$g_zxfer_run_tmp_root/prefetch-stage-1.tmp"
	second_stage_file="$g_zxfer_run_tmp_root/prefetch-stage-2.tmp"
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
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

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

	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f zxfer_group_recursive_property_tree_by_dataset
	g_cmd_awk=$old_cmd_awk
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

	assertEquals "Grouped machine/human merge failures should preserve the merge command status." \
		"17" "$prefetch_status"
	assertEquals "Grouped machine/human merge failures should surface the merge stderr." \
		"group merge failed" "$(cat "$err_log")"
	assertEquals "Grouped machine/human merge failures should disable source-side prefetch for the rest of the iteration." \
		"2" "${g_zxfer_source_property_tree_prefetch_state:-0}"
	assertEquals "Grouped machine/human merge failures should not populate the in-memory source property table." \
		"" "${g_zxfer_source_property_table:-}"

	if [ "$had_errexit" -eq 1 ]; then
		set -e
	fi
}

test_zxfer_maybe_prefetch_recursive_normalized_properties_handles_mismatches_and_missing_table_rows() {
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
	assertEquals "A prefetch pass that does not materialize the requested dataset table row should still fall back to exact live reads." \
		"1" "$missing_cache_status"
}

test_zxfer_property_table_invalidation_clears_tables_when_strip_command_fails() {
	zxfer_property_table_append_dataset destination "backup/dst" "compression=lz4=local"
	zxfer_property_table_append_dataset destination "backup/other" "atime=off=local"

	zxfer_property_table_strip_dataset_rows() {
		return 1
	}
	zxfer_property_table_invalidate_dataset destination "backup/dst" 0
	unset -f zxfer_property_table_strip_dataset_rows
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

	assertEquals "Failed table strips must fail toward the empty table so no stale row can survive an invalidation." \
		"" "${g_zxfer_destination_property_table:-}"
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
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

	{
		printf 'status=%s\n' "$prefetch_status"
		printf 'state=%s\n' "${g_zxfer_source_property_tree_prefetch_state:-0}"
	} >"$output_file"

	assertEquals "Human-tree read failures should disable recursive source property prefetch for the rest of the iteration." \
		"status=1
state=2" "$(cat "$output_file")"
}

test_zxfer_maybe_prefetch_recursive_normalized_properties_fails_when_prefetch_pass_fails() {
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
	set -e

	unset -f zxfer_prefetch_recursive_normalized_properties
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

	{
		printf 'prefetch=%s\n' "$prefetch_status"
	} >"$output_file"

	assertEquals "Destination prefetch lookups should fail cleanly when the prefetch pass itself fails." \
		"prefetch=1" "$(cat "$output_file")"
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
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

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
	table_hit=0
	if zxfer_property_table_find_dataset source "tank/src" "casesensitivity"; then
		table_hit=1
	fi

	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f zxfer_serialize_property_records_from_stdin
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

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
	assertEquals "Required-property serializer failures should not populate the required-property table." \
		"0" "$table_hit"
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
	table_hit=0
	if zxfer_property_table_find_dataset source "tank/src" "casesensitivity"; then
		table_hit=1
	fi

	unset -f zxfer_get_temp_file
	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f cat
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

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
	assertEquals "Required-property serializer readback failures should not populate the required-property table." \
		"0" "$table_hit"
}

test_zxfer_capture_serialized_property_records_reports_tempfile_failures_in_current_shell() {
	zxfer_get_temp_file() {
		return 1
	}

	set +e
	zxfer_capture_serialized_property_records "compression	lz4	local" >/dev/null 2>&1
	status=$?
	set -e

	unset -f zxfer_get_temp_file
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

	assertEquals "Serialized property capture should fail closed when it cannot allocate a staging file." \
		"1" "$status"
}

test_zxfer_capture_serialized_property_records_reports_readback_failures_in_current_shell() {
	serialized_output_file="$TEST_TMPDIR/serialized_property_readback_failure.out"
	err_log="$TEST_TMPDIR/serialized_property_readback_failure.err"
	g_zxfer_serialized_property_records_result="stale-serialized"

	zxfer_get_temp_file() {
		g_zxfer_temp_file_result="$serialized_output_file"
		: >"$g_zxfer_temp_file_result"
	}

	cat() {
		if [ "$1" = "$serialized_output_file" ]; then
			printf '%s\n' "serialized readback failed" >&2
			printf '%s\n' "compression=lz4=local"
			return 26
		fi
		command cat "$@"
	}

	set +e
	zxfer_capture_serialized_property_records "compression	lz4	local" >/dev/null 2>"$err_log"
	status=$?
	set -e

	unset -f zxfer_get_temp_file
	unset -f cat
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

	assertEquals "Serialized property capture should fail closed when the staged serializer output cannot be read back." \
		"26" "$status"
	assertEquals "Serialized property capture should not publish stale or partial serializer scratch after a readback failure." \
		"" "$g_zxfer_serialized_property_records_result"
	assertEquals "Serialized property capture should preserve the staged readback diagnostic." \
		"serialized readback failed" "$(cat "$err_log")"
}

test_zxfer_group_recursive_property_tree_by_dataset_groups_filtered_datasets_in_order() {
	filter_file="$TEST_TMPDIR/property-filter.list"
	tree_file="$TEST_TMPDIR/property-tree.tsv"
	output_file="$TEST_TMPDIR/property-grouped.tsv"

	printf '%s\n' "tank/src" "tank/src/child" >"$filter_file"
	cat >"$tree_file" <<'EOF'
tank/src	compression	lz4	local
tank/src	readonly	off	local
tank/src/child	compression	gzip	inherited
tank/src/child	readonly	off	inherited
tank/skip	compression	off	local
EOF

	zxfer_group_recursive_property_tree_by_dataset "$filter_file" "$tree_file" >"$output_file"
	status=$?

	assertEquals "Recursive property grouping should succeed for filtered property trees." \
		"0" "$status"
	assertEquals "Recursive property grouping should retain dataset order and merge encoded property records per dataset." \
		"tank/src	compression=lz4=local,readonly=off=local
tank/src/child	compression=gzip=inherited,readonly=off=inherited" "$(cat "$output_file")"
}

test_zxfer_group_recursive_property_tree_by_dataset_preserves_line_feed_values() {
	filter_file="$TEST_TMPDIR/property-filter-linefeed.list"
	tree_file="$TEST_TMPDIR/property-tree-linefeed.tsv"
	output_file="$TEST_TMPDIR/property-grouped-linefeed.tsv"

	printf '%s\n' "tank/src" >"$filter_file"
	printf 'tank/src\tuser:note\tline1\nline2\tlocal\n' >"$tree_file"
	printf 'tank/src\tcompression\tlz4\tlocal\n' >>"$tree_file"
	printf 'tank/skip\tuser:note\tignored\nvalue\tlocal\n' >>"$tree_file"

	zxfer_group_recursive_property_tree_by_dataset "$filter_file" "$tree_file" >"$output_file"
	status=$?

	assertEquals "Recursive property grouping should accept logical property records split across physical lines." \
		"0" "$status"
	assertEquals "Recursive property grouping should encode embedded line feeds before storing grouped table records." \
		"tank/src	user:note=line1%0Aline2=local,compression=lz4=local" "$(cat "$output_file")"
}

test_zxfer_prefetch_recursive_normalized_properties_preserves_grouped_read_failures() {
	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_source_property_tree_prefetch_state=0
	g_recursive_source_dataset_list="tank/src"

	zxfer_run_zfs_cmd_for_spec() {
		case "$4" in
		-Hpo | -Ho)
			printf '%s\n' "tank/src	compression	lz4	local"
			return 0
			;;
		esac
		return 1
	}
	zxfer_read_runtime_artifact_file() {
		return 27
	}

	set +e
	zxfer_prefetch_recursive_normalized_properties source >/dev/null 2>&1
	status=$?
	set -e

	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f zxfer_read_runtime_artifact_file
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

	assertEquals "Recursive property-tree prefetch should preserve grouped artifact read failures." \
		"27" "$status"
	assertEquals "Grouped artifact read failures should disable source prefetch for the iteration." \
		"2" "${g_zxfer_source_property_tree_prefetch_state:-0}"
}

test_zxfer_prefetch_recursive_normalized_properties_skips_malformed_grouped_rows() {
	g_zxfer_source_property_tree_prefetch_root="tank/src"
	g_zxfer_source_property_tree_prefetch_zfs_cmd="/sbin/zfs"
	g_zxfer_source_property_tree_prefetch_state=0
	g_recursive_source_dataset_list="tank/src"

	zxfer_run_zfs_cmd_for_spec() {
		case "$4" in
		-Hpo | -Ho)
			printf '%s\n' "tank/src	compression	lz4	local"
			return 0
			;;
		esac
		return 1
	}
	zxfer_read_runtime_artifact_file() {
		g_zxfer_runtime_artifact_read_result="malformed-grouped-row-without-tabs"
		return 0
	}

	zxfer_prefetch_recursive_normalized_properties source >/dev/null 2>&1
	status=$?

	unset -f zxfer_run_zfs_cmd_for_spec
	unset -f zxfer_read_runtime_artifact_file
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

	assertEquals "Malformed grouped rows should be ignored without disabling recursive source prefetch." \
		"0" "$status"
	assertEquals "Successful prefetch with only skipped grouped rows should still mark source prefetch ready." \
		"1" "${g_zxfer_source_property_tree_prefetch_state:-0}"
	assertEquals "Malformed grouped rows must not leak into the in-memory source property table." \
		"" "${g_zxfer_source_property_table:-}"
}

test_zxfer_property_wrapper_helpers_preserve_exact_failure_statuses() {
	set +e
	populate_output=$(
		(
			zxfer_get_required_property_probe() {
				printf '%s\n' "populate failed"
				return 5
			}
			zxfer_populate_required_properties_present \
				"tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity" source
		)
	)
	populate_status=$?
	load_output=$(
		(
			zxfer_load_normalized_dataset_properties() {
				printf '%s\n' "normalized failed"
				return 7
			}
			zxfer_load_destination_props "backup/dst" ""
		)
	)
	load_status=$?
	collect_output=$(
		(
			zxfer_load_destination_props() {
				printf '%s\n' "collect failed"
				return 9
			}
			zxfer_collect_destination_props "backup/dst" ""
		)
	)
	collect_status=$?
	required_output=$(
		(
			zxfer_populate_required_properties_present() {
				printf '%s\n' "required failed"
				return 11
			}
			zxfer_ensure_required_properties_present \
				"tank/src" "compression=lz4=local" "/sbin/zfs" "casesensitivity" source
		)
	)
	required_status=$?

	assertEquals "Required-property population should preserve the exact failure status from required-property probes." \
		5 "$populate_status"
	assertEquals "Required-property population should preserve required-property probe failure output." \
		"populate failed" "$populate_output"
	assertEquals "Destination property loading should preserve the exact failure status from normalized-property loading." \
		7 "$load_status"
	assertEquals "Destination property loading should preserve normalized-property failure output." \
		"normalized failed" "$load_output"
	assertEquals "Destination property collection should preserve the exact failure status from destination-property loading." \
		9 "$collect_status"
	assertEquals "Destination property collection should preserve destination-property loader failure output." \
		"collect failed" "$collect_output"
	assertEquals "Required-property wrapper helpers should preserve the exact failure status from required-property population." \
		11 "$required_status"
	assertEquals "Required-property wrapper helpers should preserve required-property population failure output." \
		"required failed" "$required_output"
}

test_zxfer_load_normalized_dataset_properties_preserves_live_probe_statuses() {
	set +e
	machine_output=$(
		(
			zxfer_run_zfs_cmd_for_spec() {
				printf '%s\n' "machine probe failed"
				return 23
			}
			zxfer_load_normalized_dataset_properties "tank/src" "/sbin/zfs" source
		)
	)
	machine_status=$?
	human_output=$(
		(
			zxfer_run_zfs_cmd_for_spec() {
				if [ "$3" = "-Hpo" ]; then
					printf '%s\n' "compression	lz4	local"
					return 0
				fi
				printf '%s\n' "human probe failed"
				return 24
			}
			zxfer_load_normalized_dataset_properties "tank/src" "/sbin/zfs" source
		)
	)
	human_status=$?
	set -e

	assertEquals "Normalized-property live machine probes should preserve exact zfs status." \
		23 "$machine_status"
	assertEquals "Normalized-property live machine probes should preserve failure output." \
		"machine probe failed" "$machine_output"
	assertEquals "Normalized-property live human probes should preserve exact zfs status." \
		24 "$human_status"
	assertEquals "Normalized-property live human probes should preserve failure output." \
		"human probe failed" "$human_output"
}

test_property_state_csv_helpers_preserve_caller_ifs_and_globbing() {
	actual=$(
		(
			cd "$TEST_TMPDIR" || exit 1
			: >'value=glob=local'

			IFS=:
			set -f
			zxfer_resolve_human_vars \
				'user:note=*=local,compression=lz4=local' \
				'user:note=*=local,compression=lz4=local'
			printf 'resolved=%s\n' "$human_results"
			printf 'custom_ifs=%s\n' "$IFS"
			zxfer_property_test_report_globbing_state custom

			unset IFS
			set +f
			zxfer_get_required_property_probe() {
				g_zxfer_required_property_probe_result='normalization=formD=local'
			}
			zxfer_populate_required_properties_present 'tank/src' \
				'user:note=*=local' '/sbin/zfs' 'normalization' source
			printf 'required=%s\n' "$g_zxfer_required_properties_result"
			if [ "${IFS+set}" = set ]; then
				printf '%s\n' 'unset_ifs=set'
			else
				printf '%s\n' 'unset_ifs=unset'
			fi
			zxfer_property_test_report_globbing_state unset
		)
	)

	assertEquals "Property-state CSV parsing should not alter caller IFS/globbing or expand value globs." \
		"resolved=user:note=*=local,compression=lz4=local
custom_ifs=:
custom_globbing=disabled
required=user:note=*=local,normalization=formD=local
unset_ifs=unset
unset_globbing=enabled" "$actual"
}

# Register this fragment's tests explicitly so unfiltered shunit2 execution
# cannot depend on source scanning or evaluation.
zxfer_test_add_property_state_cache_tests() {
	suite_addTest test_readonly_property_constants_pin_source_lists
	suite_addTest test_get_effective_readonly_properties_removes_mountpoint_during_migration
	suite_addTest test_get_effective_readonly_properties_removes_mountpoint_in_current_shell
	suite_addTest test_get_effective_readonly_properties_uses_freebsd_list_when_base_is_empty
	suite_addTest test_get_effective_readonly_properties_uses_base_list_for_sunos_without_platform_delta_when_base_is_empty
	suite_addTest test_zxfer_property_owner_operations_and_state_helpers_cover_current_shell_paths
	suite_addTest test_zxfer_read_property_reconcile_stage_file_rethrows_runtime_artifact_read_failures_in_current_shell
	suite_addTest test_get_effective_readonly_properties_appends_platform_lists_when_base_is_nonempty
	suite_addTest test_unsupported_property_probe_helpers_cover_current_shell_paths
	suite_addTest test_remove_properties_preserves_override_entries
	suite_addTest test_remove_properties_trims_remaining_filter_list_with_literal_property_names
	suite_addTest test_run_zfs_create_with_properties_executes_live_create
	suite_addTest test_run_zfs_create_with_properties_allows_parent_create_without_properties
	suite_addTest test_run_zfs_create_with_properties_rejects_parent_create_with_properties
	suite_addTest test_run_zfs_create_with_properties_renders_dry_run_command
	suite_addTest test_run_zfs_create_with_properties_rejects_volume_without_size
	suite_addTest test_run_zfs_create_with_properties_decodes_delimiter_heavy_assignments_for_exec
	suite_addTest test_get_normalized_dataset_properties_defaults_to_g_lzfs
	suite_addTest test_get_normalized_dataset_properties_escapes_delimiter_heavy_values
	suite_addTest test_get_normalized_dataset_properties_escapes_line_feed_values
	suite_addTest test_get_normalized_dataset_properties_tracks_profile_counters_by_lookup_side
	suite_addTest test_get_normalized_dataset_properties_caches_same_side_dataset_in_current_shell
	suite_addTest test_get_normalized_dataset_properties_separates_source_and_destination_cache_keys
	suite_addTest test_get_normalized_dataset_properties_does_not_cache_failed_reads
	suite_addTest test_get_normalized_dataset_properties_reports_machine_serializer_failures_without_caching
	suite_addTest test_get_normalized_dataset_properties_reports_machine_serializer_readback_failures_without_caching
	suite_addTest test_get_normalized_dataset_properties_reports_human_probe_failures_without_caching
	suite_addTest test_get_normalized_dataset_properties_reports_human_serializer_failures_without_caching
	suite_addTest test_load_normalized_dataset_properties_uses_prefetched_table_row_when_available
	suite_addTest test_load_normalized_dataset_properties_falls_back_to_live_probe_when_prefetch_does_not_materialize_row
	suite_addTest test_zxfer_property_table_round_trips_hostile_dataset_names
	suite_addTest test_zxfer_invalidate_destination_property_mutation_cache_strips_descendants_and_keeps_siblings
	suite_addTest test_zxfer_invalidate_destination_property_mutation_cache_without_dataset_resets_destination_tables
	suite_addTest test_zxfer_reset_property_iteration_caches_clears_tables_memo_and_prefetch_state
	suite_addTest test_zxfer_property_table_invalidate_dataset_removes_exact_source_rows_only
	suite_addTest test_zxfer_property_table_invalidate_dataset_ignores_unknown_sides
	suite_addTest test_zxfer_reset_destination_property_iteration_cache_preserves_source_table_rows
	suite_addTest test_zxfer_reset_destination_property_iteration_cache_rearms_destination_tree_prefetch
	suite_addTest test_zxfer_refresh_property_tree_prefetch_context_tracks_recursive_property_roots
	suite_addTest test_zxfer_refresh_property_tree_prefetch_context_clears_state_when_prefetch_is_inapplicable
	suite_addTest test_zxfer_get_property_tree_prefetch_dataset_list_uses_source_and_destination_fallbacks
	suite_addTest test_get_normalized_dataset_properties_prefetches_recursive_source_tree_and_slices_locally
	suite_addTest test_get_normalized_dataset_properties_prefetches_recursive_destination_tree_and_slices_locally
	suite_addTest test_adjust_child_inherit_to_match_parent_uses_prefetched_destination_tree
	suite_addTest test_zxfer_prefetch_recursive_normalized_properties_handles_invalid_side_and_state_shortcuts
	suite_addTest test_zxfer_prefetch_recursive_normalized_properties_disables_missing_context_and_empty_filters
	suite_addTest test_zxfer_prefetch_recursive_normalized_properties_disables_destination_missing_context_and_empty_filters
	suite_addTest test_zxfer_prefetch_recursive_normalized_properties_disables_failed_recursive_reads_and_grouping
	suite_addTest test_zxfer_prefetch_recursive_normalized_properties_disables_failed_destination_reads_and_grouping
	suite_addTest test_zxfer_prefetch_recursive_normalized_properties_rethrows_tempfile_allocation_failures
	suite_addTest test_zxfer_prefetch_recursive_normalized_properties_rethrows_grouped_merge_failures
	suite_addTest test_zxfer_maybe_prefetch_recursive_normalized_properties_handles_mismatches_and_missing_table_rows
	suite_addTest test_zxfer_property_table_invalidation_clears_tables_when_strip_command_fails
	suite_addTest test_zxfer_prefetch_recursive_normalized_properties_disables_human_tree_read_failures
	suite_addTest test_zxfer_maybe_prefetch_recursive_normalized_properties_fails_when_prefetch_pass_fails
	suite_addTest test_zxfer_get_required_property_probe_defaults_to_local_zfs_command_when_unspecified
	suite_addTest test_zxfer_get_required_property_probe_reports_serializer_failures_without_caching
	suite_addTest test_zxfer_get_required_property_probe_reports_serializer_readback_failures_without_caching
	suite_addTest test_zxfer_capture_serialized_property_records_reports_tempfile_failures_in_current_shell
	suite_addTest test_zxfer_capture_serialized_property_records_reports_readback_failures_in_current_shell
	suite_addTest test_zxfer_group_recursive_property_tree_by_dataset_groups_filtered_datasets_in_order
	suite_addTest test_zxfer_group_recursive_property_tree_by_dataset_preserves_line_feed_values
	suite_addTest test_zxfer_prefetch_recursive_normalized_properties_preserves_grouped_read_failures
	suite_addTest test_zxfer_prefetch_recursive_normalized_properties_skips_malformed_grouped_rows
	suite_addTest test_zxfer_property_wrapper_helpers_preserve_exact_failure_statuses
	suite_addTest test_zxfer_load_normalized_dataset_properties_preserves_live_probe_statuses
	suite_addTest test_property_state_csv_helpers_preserve_caller_ifs_and_globbing
}
