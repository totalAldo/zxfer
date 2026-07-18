#!/bin/sh
# Property source collection, policy, compatibility, and metadata behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

test_force_readonly_off_handles_empty_and_rewrites_property() {
	assertEquals "Empty property lists should stay empty." "" "$(zxfer_force_readonly_off "")"
	assertEquals "readonly=on entries should be forced to readonly=off." \
		"readonly=off=local,compression=lz4=local" \
		"$(zxfer_force_readonly_off "readonly=on=local,compression=lz4=local")"
}

test_force_readonly_off_rewrites_only_readonly_property_entries() {
	input="user:note=readonly=on=local,readonly=on=local,user:other=prefix-readonly=on=local,readonly=on"
	expected="user:note=readonly=on=local,readonly=off=local,user:other=prefix-readonly=on=local,readonly=off"

	assertEquals "Writable-mode forcing should not rewrite user-property values that merely contain readonly=on." \
		"$expected" "$(zxfer_force_readonly_off "$input")"
}

test_collect_source_props_uses_backup_restore_and_force_writable() {
	output_file="$TEST_TMPDIR/collect_source_restore.out"

	(
		zxfer_get_normalized_dataset_properties() {
			printf '%s\n' "compression=lz4=local,readonly=on=local"
		}
		g_option_e_restore_property_mode=1
		ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
		ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
		g_restored_backup_file_contents=$(zxfer_test_render_current_backup_metadata_contents \
			"$(zxfer_test_backup_metadata_row "." "readonly=on=local,compression=lz4=local")")
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
			ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
			ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/other"
			g_restored_backup_file_contents=$(zxfer_test_render_current_backup_metadata_contents \
				"$(zxfer_test_backup_metadata_row "." "compression=off=local")")
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
				"#format_version:2" \
				"#version:test-version" \
				"$(zxfer_test_backup_metadata_row "." "compression=off=local")")
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
				"$(zxfer_test_backup_metadata_row "." "compression=off=local")")
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
		)
	)
	status=$?

	assertEquals "Restore-mode source collection should fail closed on unsupported backup metadata schema versions." \
		2 "$status"
	assertContains "Unknown-format restore failures should identify the expected schema marker." \
		"$output" "Restored properties for the filesystem tank/src and destination backup/dst do not declare supported zxfer backup metadata format version #format_version:2"
}

test_collect_source_props_propagates_normalized_property_lookup_failures() {
	set +e
	output=$(
		(
			zxfer_get_normalized_dataset_properties() {
				printf '%s\n' "permission denied"
				return 27
			}
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
		)
	)
	status=$?

	assertEquals "Source property collection should preserve normalized source inspection failures." \
		"27" "$status"
	assertEquals "Source property collection should preserve the normalized-property lookup failure output." \
		"permission denied" "$output"
}

test_collect_source_props_rethrows_staged_readback_failures_after_successful_lookup() {
	set +e
	output=$(
		(
			l_tmp_path="$g_zxfer_run_tmp_root/collect_source_readback_success.tmp"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$l_tmp_path
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_get_normalized_dataset_properties() {
				printf '%s\n' "compression=lz4=local"
			}
			zxfer_read_property_reconcile_stage_file() {
				return 23
			}
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
			l_status=$?
			printf 'status=%s\n' "$l_status"
			if [ -e "$l_tmp_path" ]; then
				printf 'tmp_exists=yes\n'
			else
				printf 'tmp_exists=no\n'
			fi
			printf 'raw=<%s>\n' "${g_zxfer_source_pvs_raw:-}"
			printf 'effective=<%s>\n' "${g_zxfer_source_pvs_effective:-}"
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Successful source property collection should still fail closed when staged readback fails." \
		"23" "$status"
	assertContains "Successful source-property staged readback failures should clean the staged temp file." \
		"$output" "tmp_exists=no"
	assertContains "Successful source-property staged readback failures should not publish partial raw properties." \
		"$output" "raw=<>"
	assertContains "Successful source-property staged readback failures should not publish partial effective properties." \
		"$output" "effective=<>"
}

test_collect_source_props_rethrows_staged_readback_failures_after_failed_lookup() {
	set +e
	output=$(
		(
			l_tmp_path="$g_zxfer_run_tmp_root/collect_source_readback_failure.tmp"
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$l_tmp_path
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_get_normalized_dataset_properties() {
				printf '%s\n' "permission denied"
				return 1
			}
			zxfer_read_property_reconcile_stage_file() {
				return 24
			}
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
			l_status=$?
			printf 'status=%s\n' "$l_status"
			if [ -e "$l_tmp_path" ]; then
				printf 'tmp_exists=yes\n'
			else
				printf 'tmp_exists=no\n'
			fi
			printf 'raw=<%s>\n' "${g_zxfer_source_pvs_raw:-}"
			printf 'effective=<%s>\n' "${g_zxfer_source_pvs_effective:-}"
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Failed source property collection should preserve staged readback failures instead of degrading to the original lookup status." \
		"24" "$status"
	assertContains "Failed source-property staged readback failures should clean the staged temp file." \
		"$output" "tmp_exists=no"
	assertContains "Failed source-property staged readback failures should not publish partial raw properties." \
		"$output" "raw=<>"
	assertContains "Failed source-property staged readback failures should not publish partial effective properties." \
		"$output" "effective=<>"
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

test_collect_source_props_restore_mode_reports_unexpected_metadata_validation_failures() {
	set +e
	output=$(
		(
			zxfer_get_normalized_dataset_properties() {
				printf '%s\n' "compression=lz4=local"
			}
			zxfer_validate_backup_metadata_format() {
				return 9
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			g_restored_backup_file_contents="restored metadata"
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
		)
	)
	status=$?

	assertEquals "Restore-mode source collection should fail closed on unexpected backup-metadata validation failures." \
		2 "$status"
	assertContains "Unexpected backup-metadata validation failures should use the dedicated generic validation message." \
		"$output" "Failed to validate the restored backup metadata for the filesystem tank/src and destination backup/dst"
}

test_collect_source_props_restore_mode_reports_unexpected_metadata_extract_failures() {
	set +e
	output=$(
		(
			zxfer_get_normalized_dataset_properties() {
				printf '%s\n' "compression=lz4=local"
			}
			zxfer_validate_backup_metadata_format() {
				return 0
			}
			zxfer_backup_metadata_extract_properties_for_dataset_pair() {
				return 9
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			g_restored_backup_file_contents="restored metadata"
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
		)
	)
	status=$?

	assertEquals "Restore-mode source collection should fail closed on unexpected restored-property parse failures." \
		2 "$status"
	assertContains "Unexpected restored-property parse failures should use the dedicated generic parse message." \
		"$output" "Failed to parse the restored properties for the filesystem tank/src and destination backup/dst"
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
			ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
			ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
			g_restored_backup_file_contents=$(zxfer_test_render_current_backup_metadata_contents \
				"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
				"$(zxfer_test_backup_metadata_row "." "compression=off=local")")
			zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
		)
	)
	status=$?

	assertEquals "Restore-mode source collection should reject duplicate v2 relative backup rows." \
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
		ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
		ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
		g_restored_backup_file_contents=$(zxfer_test_render_current_backup_metadata_contents \
			"$(zxfer_test_backup_metadata_row "child.tail-010" "user:note=value%2Cwith%2Ccommas=local")" \
			"$(zxfer_test_backup_metadata_row "child.tail-01" "user:note=value%3Dwith%3Dequals%3Band%3Bsemicolon=local")")
		zxfer_collect_source_props "tank/src/child.tail-01" "backup/dst/child.tail-01" 0 ""
		printf '%s\n' "$g_zxfer_source_pvs_effective" >"$output_file"
	)

	assertEquals "Restore-mode source matching should select the exact awkward dataset tail and preserve the encoded serialized payload." \
		"user:note=value%3Dwith%3Dequals%3Band%3Bsemicolon=local" "$(cat "$output_file")"
}

test_collect_source_props_restore_mode_uses_exact_backup_entry_in_current_shell() {
	zxfer_get_normalized_dataset_properties() {
		printf '%s\n' "compression=lz4=local"
	}
	g_option_e_restore_property_mode=1
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
	g_restored_backup_file_contents=$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=off=local")" \
		"$(zxfer_test_backup_metadata_row "other" "compression=on=local")")
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT
	unset ZXFER_TEST_BACKUP_DESTINATION_ROOT

	# shellcheck disable=SC2218
	zxfer_collect_source_props "tank/src" "backup/dst" 0 ""
	raw_result=$g_zxfer_source_pvs_raw
	effective_result=$g_zxfer_source_pvs_effective

	# shellcheck source=src/zxfer_property_reconcile.sh
	. "$ZXFER_ROOT/src/zxfer_property_reconcile.sh"

	assertEquals "Restore-mode source collection should keep the live source property list as the raw property set." \
		"compression=lz4=local" "$raw_result"
	assertEquals "Restore-mode source collection should extract the v2 relative backup row into the effective property set." \
		"compression=off=local" "$effective_result"
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
	assertContains "Override validation failures should identify missing source properties separately from malformed -o syntax." \
		"$output" "Missing source property for -o override: quota."
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
	assertEquals "Override-only derivation should use the requested override list as the missing-dataset creation plan." \
		"compression=lz4=override,quota=1G=override" "$(sed -n '2p' "$output_file")"
}

test_derive_override_lists_preserves_required_create_props_when_transfer_all_disabled() {
	output_file="$TEST_TMPDIR/derive_override_required_create.out"

	zxfer_derive_override_lists \
		"compression=off=local,casesensitivity=sensitive=local,normalization=formD=local,utf8only=on=local,quota=1G=local" \
		"compression=lz4" \
		"0" \
		"filesystem" >"$output_file"

	assertEquals "Override-only derivation should preserve requested overrides and add required create properties to the apply list." \
		"compression=lz4=override,casesensitivity=sensitive=local,normalization=formD=local,utf8only=on=local" "$(sed -n '1p' "$output_file")"
	assertEquals "Override-only derivation should keep explicit overrides plus source must-create properties for missing-dataset creation." \
		"compression=lz4=override,casesensitivity=sensitive=local,normalization=formD=local,utf8only=on=local" "$(sed -n '2p' "$output_file")"
}

test_derive_override_lists_uses_required_create_override_for_creation() {
	output_file="$TEST_TMPDIR/derive_override_required_create_override.out"

	zxfer_derive_override_lists \
		"casesensitivity=sensitive=local,compression=off=local" \
		"casesensitivity=insensitive" \
		"0" \
		"filesystem" >"$output_file"

	assertEquals "Override-only derivation should keep the user override in the apply list." \
		"casesensitivity=insensitive=override" "$(sed -n '1p' "$output_file")"
	assertEquals "Missing-dataset creation should use the override value when a required create property is explicitly overridden." \
		"casesensitivity=insensitive=override" "$(sed -n '2p' "$output_file")"
}

test_derive_override_lists_uses_explicit_override_for_inherited_creation() {
	output_file="$TEST_TMPDIR/derive_override_inherited_create_override.out"

	zxfer_derive_override_lists \
		"compression=off=local,atime=on=inherited" \
		"atime=off" \
		"0" \
		"filesystem" >"$output_file"

	assertEquals "Override-only derivation should keep explicit inherited-property overrides in the apply list." \
		"atime=off=override" "$(sed -n '1p' "$output_file")"
	assertEquals "Missing-dataset creation should keep explicit overrides even when the source property was inherited." \
		"atime=off=override" "$(sed -n '2p' "$output_file")"
}

test_derive_override_lists_uses_explicit_override_absent_from_source_for_creation() {
	output_file="$TEST_TMPDIR/derive_override_absent_create_override.out"

	zxfer_derive_override_lists \
		"compression=off=local" \
		"user:note=replicated" \
		"0" \
		"filesystem" >"$output_file"

	assertEquals "Override-only derivation should keep explicit overrides even when this source dataset omits the property." \
		"user:note=replicated=override" "$(sed -n '1p' "$output_file")"
	assertEquals "Missing-dataset creation should still carry explicit overrides that are absent from the current source dataset." \
		"user:note=replicated=override" "$(sed -n '2p' "$output_file")"
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
	assertEquals "Transfer-all derivation should keep overridden local properties and zvol refreservation in the creation-property set." \
		"compression=gzip-9=override,refreservation=8G=received" "$(sed -n '2p' "$output_file")"
}

test_derive_override_lists_escapes_override_values_after_first_equals() {
	output_file="$TEST_TMPDIR/derive_override_delimiter_values.out"

	zxfer_derive_override_lists "" "user:note=value=with=equals;semi" "0" "filesystem" >"$output_file"

	assertEquals "Override derivation should preserve the full override value by escaping internal delimiters after the first equals sign." \
		"user:note=value%3Dwith%3Dequals%3Bsemi=override" "$(sed -n '1p' "$output_file")"
	assertEquals "Override-only derivation should preserve delimiter-heavy values in the creation-property list." \
		"user:note=value%3Dwith%3Dequals%3Bsemi=override" "$(sed -n '2p' "$output_file")"
}

test_derive_override_lists_escapes_literal_commas_inside_override_values() {
	output_file="$TEST_TMPDIR/derive_override_escaped_commas.out"

	zxfer_derive_override_lists "" "user:note=value\\,with\\,commas=and;semi" "0" "filesystem" >"$output_file"

	assertEquals "Override derivation should decode escaped commas before storing the internal encoded value." \
		"user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=override" "$(sed -n '1p' "$output_file")"
	assertEquals "Override-only derivation should preserve escaped-comma values in the creation-property list." \
		"user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=override" "$(sed -n '2p' "$output_file")"
}

test_derive_override_lists_preserves_literal_backslashes() {
	output_file="$TEST_TMPDIR/derive_override_backslashes.out"

	zxfer_derive_override_lists "" 'user:path=C:\\temp\\logs' "0" "filesystem" >"$output_file"

	assertEquals "Override derivation should not collapse literal backslashes that are not escaping commas." \
		'user:path=C:\\temp\\logs=override' "$(sed -n '1p' "$output_file")"
	assertEquals "Backslash-only values should be preserved in the creation-property list in override-only mode." \
		'user:path=C:\\temp\\logs=override' "$(sed -n '2p' "$output_file")"
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
		"$(zxfer_sanitize_property_list "" "$ZXFER_BASE_READONLY_PROPERTIES" "$g_option_I_ignore_properties")"
}

test_strip_unsupported_properties_returns_input_when_no_unsupported_properties() {
	assertEquals "Unsupported-property stripping should no-op when no unsupported list is present." \
		"compression=lz4=local" "$(zxfer_strip_unsupported_properties "compression=lz4=local" "")"
}

test_strip_unsupported_properties_honors_explicit_unsupported_list_argument() {
	assertEquals "Unsupported-property stripping should honor the explicit unsupported list it is passed." \
		"compression=lz4=local" "$(zxfer_strip_unsupported_properties "compression=lz4=local,quota=1G=local" "quota")"
}

test_remove_unsupported_properties_honors_explicit_unsupported_list_argument() {
	zxfer_remove_unsupported_properties "compression=lz4=local,quota=1G=local" "quota"

	assertEquals "Unsupported-property filtering should honor the explicit unsupported list it is passed." \
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

test_remove_unsupported_properties_preserves_nonthrowing_tempfile_status() {
	set +e
	output=$(
		(
			g_zxfer_only_supported_properties="stale"
			g_zxfer_property_reconcile_stage_file_result="stale-stage"
			zxfer_get_temp_file() {
				return 37
			}
			zxfer_remove_unsupported_properties "compression=lz4=local,quota=1G=local" "quota"
			l_status=$?
			printf 'status=%s\n' "$l_status"
			printf 'result=%s\n' "$g_zxfer_only_supported_properties"
			printf 'stage=%s\n' "$g_zxfer_property_reconcile_stage_file_result"
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Unsupported-property filtering should preserve non-throwing temp allocation statuses." \
		37 "$status"
	assertEquals "Unsupported-property filtering should clear stale output and stage-file allocation results on failure." \
		"status=37
result=
stage=" "$output"
}

test_strip_unsupported_properties_keeps_stdout_clean_when_verbose() {
	stdout_log="$TEST_TMPDIR/unsupported_stdout.log"
	stderr_log="$TEST_TMPDIR/unsupported_stderr.log"
	unsupported_properties="compression"
	g_option_v_verbose=1

	zxfer_strip_unsupported_properties "compression=lz4=local,quota=1G=local" "$unsupported_properties" >"$stdout_log" 2>"$stderr_log"

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

	zxfer_strip_unsupported_properties "user:note=value%2Cwith%2Ccommas%3Dand%3Bsemi=local" "$unsupported_properties" >"$stdout_log" 2>"$stderr_log"

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
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$TEST_TMPDIR/unsupported_awk_failure.tmp"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_throw_error() {
				printf '%s\n' "$1" >&2
				exit 1
			}
			zxfer_strip_unsupported_properties "compression=lz4=local" "$unsupported_properties"
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
			l_tmp_path="$g_zxfer_run_tmp_root/unsupported_readback_failure.tmp"
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
		zxfer_select_unsupported_properties_for_dataset_type filesystem
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
			if [ "$*" = "get -Hpo value type tank/src" ]; then
				printf 'filesystem\n'
				return 0
			fi
			if [ "$*" = "get -Hpo property all tank/src" ]; then
				printf 'compression\n'
			fi
		}
		zxfer_calculate_unsupported_properties
		zxfer_select_unsupported_properties_for_dataset_type filesystem
	) >"$TEST_TMPDIR/unsupported_pool_props.out"

	assertEquals "Missing destination roots should fall back to the destination pool for unsupported-property probes." \
		"" "$(cat "$TEST_TMPDIR/unsupported_pool_props.out")"
	assertContains "Missing destination roots should probe the destination pool instead of the absent dataset path." \
		"$(cat "$probe_log")" "get -Hpo property,value,source compression backup"
}

test_calculate_unsupported_properties_reports_blank_pool_fallback_probe_failures() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"
	probe_log="$TEST_TMPDIR/unsupported_blank_pool_probe.log"
	: >"$probe_log"

	set +e
	output=$(
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
			zxfer_calculate_unsupported_properties
		) 2>&1
	)
	status=$?

	assertEquals "Blank pool-root property probe failures should fail closed on the OpenZFS 2+ floor instead of using the legacy illumos fallback." \
		"1" "$status"
	assertContains "Blank pool-root property probe failures should be reported as ambiguous probe failures." \
		"$output" "Failed to probe destination support for property [compression] on [backup]: probe exited nonzero without stdout/stderr"
	assertNotContains "Blank pool-root probe failures should not run the removed legacy queryability probe." \
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

test_calculate_unsupported_properties_keeps_dataset_type_caches_without_compatibility_union() {
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
		printf 'filesystem=%s\n' "$(zxfer_select_unsupported_properties_for_dataset_type filesystem)"
		printf 'volume=%s\n' "$(zxfer_select_unsupported_properties_for_dataset_type volume)"
	) >"$TEST_TMPDIR/unsupported_union_props.out"

	assertEquals "Unsupported-property calculation should cache filesystem and volume entries without publishing a merged active list." \
		"filesystem=recordsize
volume=volblocksize" "$(cat "$TEST_TMPDIR/unsupported_union_props.out")"
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
		printf 'filesystem=%s\n' "$(zxfer_select_unsupported_properties_for_dataset_type filesystem)"
		printf 'volume=%s\n' "$(zxfer_select_unsupported_properties_for_dataset_type volume)"
	) >"$TEST_TMPDIR/unsupported_recursive_props.out"

	assertEquals "Recursive unsupported-property scans should include child dataset properties and keep only dataset-type caches active." \
		"filesystem=
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
		printf 'volume=%s\n' "$(zxfer_select_unsupported_properties_for_dataset_type volume)"
	) >"$TEST_TMPDIR/unsupported_volume_fallback.out"

	assertEquals "Pool-root fallback probes should not mark valid volume-only properties unsupported just because the fallback dataset type is filesystem." \
		"volume=" "$(cat "$TEST_TMPDIR/unsupported_volume_fallback.out")"
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
		printf 'volume=%s\n' "$(zxfer_select_unsupported_properties_for_dataset_type volume)"
	) >"$TEST_TMPDIR/unsupported_child_destination_probe.out"

	assertEquals "Recursive unsupported-property scans should probe against an existing child destination dataset when its type differs from the destination root." \
		"volume=volblocksize" "$(cat "$TEST_TMPDIR/unsupported_child_destination_probe.out")"
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
		printf 'volume=%s\n' "$(zxfer_select_unsupported_properties_for_dataset_type volume)"
	) >"$TEST_TMPDIR/unsupported_inconclusive_retry.out"

	assertEquals "Inconclusive unsupported-property probes should not prevent a later authoritative probe for the same dataset type and property." \
		"volume=volblocksize" "$(cat "$TEST_TMPDIR/unsupported_inconclusive_retry.out")"
	assertContains "Later matching-type destinations should still be probed after an earlier pool-root fallback was inconclusive." \
		"$(cat "$probe_log")" "get -Hpo property,value,source volblocksize backup/dst/vol-existing"
}

test_calculate_unsupported_properties_skips_duplicate_property_type_pairs_after_first_authoritative_probe() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src tank/src/child"
	g_destination="backup/dst"
	probe_log="$TEST_TMPDIR/unsupported_duplicate_pair_probe.log"
	: >"$probe_log"

	(
		PROBE_LOG="$probe_log"
		zxfer_exists_destination() {
			case "$1" in
			backup/dst | backup/dst/child)
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
			"get -Hpo value type backup/dst" | "get -Hpo value type backup/dst/child")
				printf 'filesystem\n'
				;;
			"get -Hpo property,value,source compression backup/dst")
				printf 'compression\tlz4\tlocal\n'
				;;
			esac
		}
		zxfer_run_source_zfs_cmd() {
			case "$*" in
			"get -Hpo value type tank/src" | "get -Hpo value type tank/src/child")
				printf 'filesystem\n'
				;;
			"get -Hpo property all tank/src" | "get -Hpo property all tank/src/child")
				printf 'compression\n'
				;;
			esac
		}
		zxfer_calculate_unsupported_properties
		zxfer_select_unsupported_properties_for_dataset_type filesystem
	) >"$TEST_TMPDIR/unsupported_duplicate_pair_props.out"

	assertEquals "Repeated source datasets with the same type/property pair should reuse the first authoritative destination probe result." \
		"" "$(cat "$TEST_TMPDIR/unsupported_duplicate_pair_props.out")"
	assertEquals "Resolved unsupported-property pairs should not trigger a second destination property probe for later datasets of the same type." \
		"1" "$(grep -c '^get -Hpo property,value,source compression ' "$probe_log")"
	assertNotContains "Duplicate type/property pairs should skip the child destination property probe once the first authoritative probe has been cached." \
		"$(cat "$probe_log")" "get -Hpo property,value,source compression backup/dst/child"
}

test_calculate_unsupported_properties_treats_matching_type_inconclusive_probes_as_unsupported() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src/childvol"
	g_destination="backup/dst"
	probe_log="$TEST_TMPDIR/unsupported_matching_type_inconclusive_probe.log"
	: >"$probe_log"

	(
		PROBE_LOG="$probe_log"
		zxfer_exists_destination() {
			case "$1" in
			backup/dst/childvol)
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
			"get -Hpo value type backup/dst/childvol")
				printf 'volume\n'
				;;
			"get -Hpo property,value,source volblocksize backup/dst/childvol")
				printf '%s\n' "property does not apply to datasets of this type"
				return 1
				;;
			esac
		}
		zxfer_run_source_zfs_cmd() {
			case "$*" in
			"get -Hpo value type tank/src/childvol")
				printf 'volume\n'
				;;
			"get -Hpo property all tank/src/childvol")
				printf 'volblocksize\n'
				;;
			esac
		}
		zxfer_calculate_unsupported_properties
		printf 'volume=%s\n' "$(zxfer_select_unsupported_properties_for_dataset_type volume)"
	) >"$TEST_TMPDIR/unsupported_matching_type_inconclusive.out"

	assertEquals "Matching-type inconclusive destination probes should still classify the property as unsupported." \
		"volume=volblocksize" "$(cat "$TEST_TMPDIR/unsupported_matching_type_inconclusive.out")"
	assertContains "Matching-type inconclusive probes should use the existing child destination dataset as the probe target." \
		"$(cat "$probe_log")" "get -Hpo property,value,source volblocksize backup/dst/childvol"
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

test_calculate_unsupported_properties_fails_closed_when_probe_dataset_lookup_fails() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"

	set +e
	output=$(
		(
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
			zxfer_get_unsupported_property_probe_dataset_for_source() {
				printf '%s\n' "probe dataset failure"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_calculate_unsupported_properties
		) 2>&1
	)
	status=$?

	assertEquals "Unsupported-property scanning should fail closed when the probe dataset lookup fails." \
		"1" "$status"
	assertEquals "Unsupported-property scanning should preserve the probe dataset lookup failure." \
		"probe dataset failure" "$output"
}

test_calculate_unsupported_properties_fails_closed_when_probe_dataset_type_lookup_fails() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"

	set +e
	output=$(
		(
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
			zxfer_get_unsupported_property_probe_dataset_for_source() {
				printf '%s\n' "backup/dst"
			}
			zxfer_get_unsupported_property_probe_dataset_type() {
				printf '%s\n' "probe dataset type failure"
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_calculate_unsupported_properties
		) 2>&1
	)
	status=$?

	assertEquals "Unsupported-property scanning should fail closed when the destination probe dataset type lookup fails." \
		"1" "$status"
	assertEquals "Unsupported-property scanning should preserve the probe dataset type lookup failure." \
		"probe dataset type failure" "$output"
}

test_calculate_unsupported_properties_fails_closed_when_source_property_staging_fails() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"

	set +e
	output=$(
		(
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
			zxfer_get_unsupported_property_probe_dataset_for_source() {
				printf '%s\n' "backup/dst"
			}
			zxfer_get_unsupported_property_probe_dataset_type() {
				printf '%s\n' "filesystem"
			}
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result="$TEST_TMPDIR/unsupported_stage_source_props.tmp"
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_write_runtime_artifact_file() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_calculate_unsupported_properties
		) 2>&1
	)
	status=$?

	assertEquals "Unsupported-property scanning should fail closed when the source property stage file cannot be written." \
		"1" "$status"
	assertEquals "Unsupported-property scanning should report the staged source property list failure." \
		"Failed to stage source property list for unsupported-property scan [tank/src]." "$output"
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

test_property_policy_csv_helpers_preserve_caller_ifs_and_globbing() {
	actual=$(
		(
			cd "$TEST_TMPDIR" || exit 1
			: >'user:note=expanded=local'

			IFS=:
			set -f
			zxfer_sanitize_property_list \
				'user:note=*=local,readonly=on=local,compression=lz4=local' \
				'readonly' 'atime' >sanitized.out
			printf 'sanitized=%s\n' "$(cat sanitized.out)"
			printf 'custom_ifs=%s\n' "$IFS"
			zxfer_property_test_report_globbing_state custom

			unset IFS
			set +f
			zxfer_remove_sources 'user:note=*=local,compression=lz4=local'
			printf 'sources=%s\n' "$g_zxfer_new_rmvs_pv"
			zxfer_force_readonly_off \
				'user:note=*=local,readonly=on=local' >forced.out
			printf 'forced=%s\n' "$(cat forced.out)"
			if [ "${IFS+set}" = set ]; then
				printf '%s\n' 'unset_ifs=set'
			else
				printf '%s\n' 'unset_ifs=unset'
			fi
			zxfer_property_test_report_globbing_state unset
		)
	)

	assertEquals "Property-policy CSV parsing should not alter caller IFS/globbing or expand value globs." \
		"sanitized=user:note=*=local,compression=lz4=local
custom_ifs=:
custom_globbing=disabled
sources=user:note=*,compression=lz4
forced=user:note=*=local,readonly=off=local
unset_ifs=unset
unset_globbing=enabled" "$actual"
}

test_calculate_unsupported_properties_preserves_caller_ifs_and_globbing() {
	actual=$(
		(
			SCAN_LOG=$TEST_TMPDIR/unsupported_property_shell_state.log
			: >"$SCAN_LOG"
			g_recursive_source_list='tank/src tank/src/child'
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = type ]; then
					printf '%s\n' filesystem
				else
					printf '%s\n' compression
				fi
			}
			zxfer_get_unsupported_property_probe_dataset_for_source() {
				printf '%s\n' 'backup/dst'
			}
			zxfer_get_unsupported_property_probe_dataset_type() {
				printf '%s\n' filesystem
			}
			zxfer_load_unsupported_property_source_list() {
				printf '%s\n' "$1" >>"$SCAN_LOG"
				g_zxfer_property_stage_file_read_result=$2
			}
			zxfer_probe_unsupported_properties_for_dataset() {
				l_probe_error=""
			}

			IFS=:
			set -f
			zxfer_calculate_unsupported_properties
			printf 'custom_sources=%s\n' "$(tr '\n' ',' <"$SCAN_LOG" | sed 's/,$//')"
			printf 'custom_ifs=%s\n' "$IFS"
			zxfer_property_test_report_globbing_state custom

			: >"$SCAN_LOG"
			unset IFS
			set +f
			zxfer_calculate_unsupported_properties
			printf 'unset_sources=%s\n' "$(tr '\n' ',' <"$SCAN_LOG" | sed 's/,$//')"
			if [ "${IFS+set}" = set ]; then
				printf '%s\n' 'unset_ifs=set'
			else
				printf '%s\n' 'unset_ifs=unset'
			fi
			zxfer_property_test_report_globbing_state unset
		)
	)

	assertEquals "Unsupported-property source scans should split their list independently and preserve caller shell state." \
		"custom_sources=tank/src,tank/src/child
custom_ifs=:
custom_globbing=disabled
unset_sources=tank/src,tank/src/child
unset_ifs=unset
unset_globbing=enabled" "$actual"
}

# Register this fragment's tests explicitly so unfiltered shunit2 execution
# cannot depend on source scanning or evaluation.
zxfer_test_add_property_policy_tests() {
	suite_addTest test_force_readonly_off_handles_empty_and_rewrites_property
	suite_addTest test_force_readonly_off_rewrites_only_readonly_property_entries
	suite_addTest test_collect_source_props_uses_backup_restore_and_force_writable
	suite_addTest test_collect_source_props_restore_mode_requires_exact_destination_match
	suite_addTest test_collect_source_props_fails_when_backup_entry_missing
	suite_addTest test_collect_source_props_restore_mode_requires_backup_metadata_header
	suite_addTest test_collect_source_props_restore_mode_requires_backup_metadata_header_first
	suite_addTest test_collect_source_props_restore_mode_rejects_unknown_backup_metadata_format_version
	suite_addTest test_collect_source_props_propagates_normalized_property_lookup_failures
	suite_addTest test_collect_source_props_rethrows_staged_readback_failures_after_successful_lookup
	suite_addTest test_collect_source_props_rethrows_staged_readback_failures_after_failed_lookup
	suite_addTest test_collect_source_props_rethrows_tempfile_allocation_failures
	suite_addTest test_collect_source_props_restore_mode_reports_unexpected_metadata_validation_failures
	suite_addTest test_collect_source_props_restore_mode_reports_unexpected_metadata_extract_failures
	suite_addTest test_collect_source_props_rejects_ambiguous_restore_entries_for_exact_pair
	suite_addTest test_collect_source_props_restore_mode_matches_exact_awkward_dataset_tails
	suite_addTest test_collect_source_props_restore_mode_uses_exact_backup_entry_in_current_shell
	suite_addTest test_validate_override_properties_returns_success_for_empty_list_in_current_shell
	suite_addTest test_validate_override_properties_rejects_missing_source_property
	suite_addTest test_validate_override_properties_accepts_escaped_commas_in_current_shell
	suite_addTest test_validate_override_properties_rejects_missing_assignment_separator
	suite_addTest test_derive_override_lists_preserves_override_only_mode_order
	suite_addTest test_derive_override_lists_preserves_required_create_props_when_transfer_all_disabled
	suite_addTest test_derive_override_lists_uses_required_create_override_for_creation
	suite_addTest test_derive_override_lists_uses_explicit_override_for_inherited_creation
	suite_addTest test_derive_override_lists_uses_explicit_override_absent_from_source_for_creation
	suite_addTest test_derive_override_lists_prefers_first_matching_override_when_transferring_all_properties
	suite_addTest test_derive_override_lists_escapes_override_values_after_first_equals
	suite_addTest test_derive_override_lists_escapes_literal_commas_inside_override_values
	suite_addTest test_derive_override_lists_preserves_literal_backslashes
	suite_addTest test_derive_override_lists_skips_volume_only_properties_for_filesystems
	suite_addTest test_validate_override_properties_reports_awk_failures
	suite_addTest test_derive_override_lists_reports_awk_failures
	suite_addTest test_derive_override_lists_rejects_missing_assignment_separator
	suite_addTest test_sanitize_property_list_returns_empty_for_empty_input
	suite_addTest test_strip_unsupported_properties_returns_input_when_no_unsupported_properties
	suite_addTest test_strip_unsupported_properties_honors_explicit_unsupported_list_argument
	suite_addTest test_remove_unsupported_properties_honors_explicit_unsupported_list_argument
	suite_addTest test_remove_unsupported_properties_rethrows_tempfile_allocation_failures
	suite_addTest test_remove_unsupported_properties_preserves_nonthrowing_tempfile_status
	suite_addTest test_strip_unsupported_properties_keeps_stdout_clean_when_verbose
	suite_addTest test_strip_unsupported_properties_decodes_verbose_delimiter_heavy_values
	suite_addTest test_strip_unsupported_properties_reports_awk_failures
	suite_addTest test_remove_unsupported_properties_preserves_readback_failures_without_publishing_results
	suite_addTest test_calculate_unsupported_properties_uses_direct_destination_property_probes
	suite_addTest test_calculate_unsupported_properties_falls_back_to_destination_pool_when_root_is_missing
	suite_addTest test_calculate_unsupported_properties_reports_blank_pool_fallback_probe_failures
	suite_addTest test_get_unsupported_property_probe_dataset_reports_missing_destination_context
	suite_addTest test_get_unsupported_property_probe_dataset_reports_destination_lookup_failure
	suite_addTest test_get_unsupported_property_probe_dataset_type_reports_lookup_failure
	suite_addTest test_get_unsupported_property_probe_destination_for_source_reports_missing_initial_source
	suite_addTest test_get_unsupported_property_probe_destination_for_source_uses_shared_destination_mapping_helper
	suite_addTest test_get_unsupported_property_probe_destination_for_source_uses_literal_non_trailing_slash_mapping
	suite_addTest test_get_unsupported_property_probe_destination_for_source_rejects_datasets_outside_initial_tree
	suite_addTest test_get_unsupported_property_probe_dataset_for_source_propagates_mapping_failures
	suite_addTest test_append_unsupported_property_for_dataset_type_appends_without_duplicates
	suite_addTest test_calculate_unsupported_properties_keeps_dataset_type_caches_without_compatibility_union
	suite_addTest test_calculate_unsupported_properties_scans_recursive_children_and_caches_by_dataset_type
	suite_addTest test_calculate_unsupported_properties_does_not_mark_volume_properties_unsupported_when_pool_fallback_type_differs
	suite_addTest test_calculate_unsupported_properties_uses_existing_child_destination_probe_dataset_types
	suite_addTest test_calculate_unsupported_properties_retries_inconclusive_probes_until_one_is_authoritative
	suite_addTest test_calculate_unsupported_properties_skips_duplicate_property_type_pairs_after_first_authoritative_probe
	suite_addTest test_calculate_unsupported_properties_treats_matching_type_inconclusive_probes_as_unsupported
	suite_addTest test_calculate_unsupported_properties_fails_closed_on_destination_probe_error
	suite_addTest test_calculate_unsupported_properties_reports_blank_destination_probe_failures_when_destination_query_fails_too
	suite_addTest test_calculate_unsupported_properties_fails_closed_on_source_type_probe_error
	suite_addTest test_calculate_unsupported_properties_fails_closed_on_source_probe_error
	suite_addTest test_calculate_unsupported_properties_fails_closed_when_probe_dataset_lookup_fails
	suite_addTest test_calculate_unsupported_properties_fails_closed_when_probe_dataset_type_lookup_fails
	suite_addTest test_calculate_unsupported_properties_fails_closed_when_source_property_staging_fails
	suite_addTest test_calculate_unsupported_properties_rethrows_tempfile_allocation_failures
	suite_addTest test_calculate_unsupported_properties_fails_closed_on_staged_source_property_readback_error
	suite_addTest test_ensure_required_properties_present_appends_missing_creation_time_props
	suite_addTest test_ensure_required_properties_present_appends_missing_creation_time_props_to_empty_lists
	suite_addTest test_ensure_required_properties_present_tracks_backfill_probe_count_when_very_verbose
	suite_addTest test_ensure_required_properties_present_caches_explicit_probe_results_by_side_and_dataset
	suite_addTest test_zxfer_get_required_property_probe_caches_unsupported_results
	suite_addTest test_ensure_required_properties_present_skips_nonapplicable_creation_time_props
	suite_addTest test_ensure_required_properties_present_reports_parse_failures_for_malformed_probe_output
	suite_addTest test_ensure_required_properties_present_reports_probe_failures_for_required_props
	suite_addTest test_get_validated_source_dataset_create_metadata_returns_filesystem_without_volsize
	suite_addTest test_get_validated_source_dataset_create_metadata_reports_type_probe_failures
	suite_addTest test_get_validated_source_dataset_create_metadata_reports_unknown_type_output
	suite_addTest test_get_validated_source_dataset_create_metadata_requires_nonempty_volsize_for_volumes
	suite_addTest test_get_validated_source_dataset_create_metadata_reports_volsize_probe_failures
	suite_addTest test_get_required_creation_properties_for_dataset_type_skips_filesystem_only_props_for_volumes
	suite_addTest test_property_policy_csv_helpers_preserve_caller_ifs_and_globbing
	suite_addTest test_calculate_unsupported_properties_preserves_caller_ifs_and_globbing
}
