#!/bin/sh
#
# shunit2 tests for zxfer_transfer_properties.sh helpers.
#
# shellcheck disable=SC1090,SC2034,SC2317,SC2329

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

# shellcheck source=src/zxfer_globals.sh
. "$ZXFER_ROOT/src/zxfer_globals.sh"

# shellcheck source=src/zxfer_transfer_properties.sh
. "$ZXFER_ROOT/src/zxfer_transfer_properties.sh"

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
	assertContains "Missing restored property metadata should identify the source dataset." \
		"$output" "Can't find the properties for the filesystem tank/src"
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

test_collect_destination_props_defaults_to_g_rzfs() {
	result=$(
		(
			get_normalized_dataset_properties() {
				printf '%s|%s\n' "$1" "$2"
			}
			g_RZFS="/remote/zfs"
			collect_destination_props "backup/dst" ""
		)
	)

	assertEquals "Destination property collection should default to g_RZFS." \
		"backup/dst|/remote/zfs" "$result"
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

test_apply_property_changes_uses_default_runners_when_unspecified() {
	log="$TEST_TMPDIR/apply_default_runners.log"
	: >"$log"

	(
		LOG_FILE="$log"
		zxfer_run_zfs_set_property() {
			printf 'set %s=%s %s\n' "$1" "$2" "$3" >>"$LOG_FILE"
		}
		zxfer_run_zfs_inherit_property() {
			printf 'inherit %s %s\n' "$1" "$2" >>"$LOG_FILE"
		}
		apply_property_changes "backup/dst" 0 "" "compression=lz4,atime=off" "quota=none" "" ""
	)

	assertEquals "Default property runners should be used when no custom runner is supplied." \
		"set compression=lz4 backup/dst
set atime=off backup/dst
inherit quota backup/dst" "$(cat "$log")"
}

test_apply_property_changes_logs_when_child_only_inherits() {
	log="$TEST_TMPDIR/apply_inherit_only.log"
	: >"$log"
	echov() {
		printf '%s\n' "$*" >>"$log"
	}
	inherit_runner() {
		printf 'inherit %s %s\n' "$1" "$2" >>"$log"
	}

	apply_property_changes "backup/dst" 0 "" "" "quota=none" "" inherit_runner

	unset -f echov
	unset -f inherit_runner

	assertContains "Child-only inheritance changes should still log the property-update banner." \
		"$(cat "$log")" "Setting properties/sources on destination filesystem \"backup/dst\"."
	assertContains "Child-only inheritance changes should still call the inherit runner." \
		"$(cat "$log")" "inherit quota backup/dst"
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
