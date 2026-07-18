#!/bin/sh
# End-to-end property transfer orchestration and failure behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

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

test_transfer_properties_creates_destination_and_records_backup() {
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
		zxfer_exists_destination() {
			printf '%s\n' 0
		}
		zxfer_append_backup_metadata_record() {
			printf 'backup_append %s %s\n' "$1" "$2" >>"$LOG_FILE"
			g_backup_file_contents="helper-owned"
		}
		zxfer_write_backup_properties() {
			printf 'unexpected backup_write %s\n' "$g_backup_file_contents" >>"$LOG_FILE"
		}
		g_option_k_backup_property_mode=1
		g_initial_source="tank/src"
		g_actual_dest="backup/dst"
		zxfer_transfer_properties "tank/src"
		printf 'backup=%s\n' "$g_backup_file_contents" >>"$LOG_FILE"
	)

	result=$(cat "$log")
	assertContains "Initial-source transfer should validate override properties." \
		"$result" "validate  compression=lz4=local"
	assertContains "Backup mode should append raw source properties through the backup-metadata owner helper." \
		"$result" "backup_append tank/src compression=lz4=local"
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
				printf 'append %s %s\n' "$1" "$2" >>"$append_log"
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
		g_recursive_dest_list="backup/vol"
		g_zxfer_unsupported_filesystem_properties="compression"
		g_zxfer_unsupported_volume_properties="volblocksize"
		zxfer_transfer_properties "tank/vol"
	)

	assertContains "Property transfer should pass the dataset-type-specific unsupported-property list into apply-list stripping." \
		"$(cat "$log")" "strip volblocksize=16K=local || volblocksize"
	assertContains "Property transfer should also pass the dataset-type-specific unsupported-property list into create-list stripping." \
		"$(cat "$log")" "strip  || volblocksize"
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
	)

	assertEquals "Existing destinations should diff and apply property changes instead of creating the dataset." \
		"apply backup/dst 0 compression=lz4 compression=lz4" "$(cat "$log")"
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

test_transfer_properties_backfills_required_create_props_after_restore_replacement() {
	log="$TEST_TMPDIR/transfer_restore_required_create_props.log"
	: >"$log"

	(
		LOG_FILE="$log"
		g_option_e_restore_property_mode=1
		g_initial_source="tank/src"
		g_actual_dest="backup/dst"
		g_recursive_dest_list=""

		zxfer_collect_source_props() {
			g_zxfer_source_pvs_raw="compression=live=local"
			g_zxfer_source_pvs_effective="compression=restored=local"
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
			case "$2" in
			compression=live=local)
				printf '%s\n' "compression=live=local,casesensitivity=sensitive=local"
				;;
			compression=restored=local)
				printf '%s\n' "compression=restored=local,casesensitivity=sensitive=local"
				;;
			*)
				printf '%s\n' "$2"
				;;
			esac
		}
		zxfer_validate_override_properties() {
			:
		}
		zxfer_derive_override_lists() {
			printf 'derive %s\n' "$1" >>"$LOG_FILE"
			g_zxfer_override_pvs_result=$1
			g_zxfer_creation_pvs_result="casesensitivity=sensitive=local"
		}
		zxfer_sanitize_property_list() {
			printf '%s\n' "$1"
		}
		zxfer_strip_unsupported_properties() {
			printf '%s\n' "$1"
		}
		zxfer_ensure_destination_exists() {
			printf 'create override=%s creation=%s\n' "$3" "$4" >>"$LOG_FILE"
			return 0
		}
		zxfer_exists_destination() {
			printf '%s\n' 0
		}
		zxfer_capture_backup_metadata_for_completed_transfer() {
			printf 'backup %s\n' "$2" >>"$LOG_FILE"
		}

		zxfer_transfer_properties "tank/src"
	)

	result=$(cat "$log")
	assertContains "Restore-mode transfers should still backfill required create-time properties on the live raw source payload." \
		"$result" "ensure-required tank/src compression=live=local casesensitivity,normalization,utf8only"
	assertContains "Restore-mode transfers should backfill required create-time properties after replacing the effective set from backup metadata." \
		"$result" "ensure-required tank/src compression=restored=local casesensitivity,normalization,utf8only"
	assertContains "Override derivation should receive the restored and backfilled effective source properties." \
		"$result" "derive compression=restored=local,casesensitivity=sensitive=local"
	assertContains "Destination creation should use restored and backfilled effective properties." \
		"$result" "create override=compression=restored=local,casesensitivity=sensitive=local creation=casesensitivity=sensitive=local"
	assertContains "Backup capture should preserve the live raw source payload after source-side required-property discovery, not the restored effective set." \
		"$result" "backup compression=live=local,casesensitivity=sensitive=local"
}

test_transfer_properties_preserves_required_create_props_when_transfer_all_disabled() {
	log="$TEST_TMPDIR/transfer_override_only_required_create_props.log"
	: >"$log"

	(
		LOG_FILE="$log"
		g_option_P_transfer_property=0
		g_option_o_override_property="compression=lz4,atime=off"
		g_initial_source="tank/src"
		g_actual_dest="backup/dst"
		g_recursive_dest_list=""

		zxfer_collect_source_props() {
			g_zxfer_source_pvs_raw="compression=off=local,atime=on=inherited,casesensitivity=sensitive=local,normalization=formD=local,utf8only=on=local"
			g_zxfer_source_pvs_effective=$g_zxfer_source_pvs_raw
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
		zxfer_exists_destination() {
			printf '%s\n' 0
		}
		zxfer_ensure_destination_exists() {
			printf 'override=%s\n' "$3" >>"$LOG_FILE"
			printf 'creation=%s\n' "$4" >>"$LOG_FILE"
			return 0
		}
		zxfer_capture_backup_metadata_for_completed_transfer() {
			:
		}

		zxfer_transfer_properties "tank/src"
	)

	result=$(cat "$log")
	assertContains "Override-only missing root creates should keep required source create properties in the root create/apply list." \
		"$result" "override=compression=lz4=override,atime=off=override,casesensitivity=sensitive=local,normalization=formD=local,utf8only=on=local"
	assertContains "Override-only creation context should carry explicit overrides and required source create properties before child create planning filters parent-matching overrides." \
		"$result" "creation=compression=lz4=override,atime=off=override,casesensitivity=sensitive=local,normalization=formD=local,utf8only=on=local"
}

test_transfer_properties_preserves_child_override_absent_from_source_on_create() {
	log="$TEST_TMPDIR/transfer_child_absent_override_create.log"
	: >"$log"

	(
		LOG_FILE="$log"
		g_option_P_transfer_property=0
		g_option_o_override_property="user:note=replicated"
		g_initial_source="tank/src"
		g_actual_dest="backup/dst/child"
		g_recursive_dest_list=""

		zxfer_collect_source_props() {
			g_zxfer_source_pvs_raw="compression=off=local,casesensitivity=sensitive=local"
			g_zxfer_source_pvs_effective=$g_zxfer_source_pvs_raw
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
		zxfer_exists_destination() {
			printf '%s\n' 0
		}
		zxfer_ensure_destination_exists() {
			printf 'override=%s\n' "$3" >>"$LOG_FILE"
			printf 'creation=%s\n' "$4" >>"$LOG_FILE"
			return 0
		}
		zxfer_capture_backup_metadata_for_completed_transfer() {
			:
		}

		zxfer_transfer_properties "tank/src/child"
	)

	result=$(cat "$log")
	assertContains "Override-only child creates should keep explicit overrides that are absent from the child source list." \
		"$result" "override=user:note=replicated=override,casesensitivity=sensitive=local"
	assertContains "Creation context should retain absent explicit overrides before child create planning decides whether a parent can supply them by inheritance." \
		"$result" "creation=user:note=replicated=override,casesensitivity=sensitive=local"
}

test_transfer_properties_strips_unsupported_creation_props_when_requested() {
	log="$TEST_TMPDIR/transfer_creation_skip_unsupported.log"
	: >"$log"

	(
		LOG_FILE="$log"
		g_option_P_transfer_property=0
		g_option_U_skip_unsupported_properties=1
		g_option_o_override_property="compression=lz4"
		g_initial_source="tank/src"
		g_actual_dest="backup/dst"
		g_recursive_dest_list=""
		g_zxfer_unsupported_filesystem_properties="casesensitivity"

		zxfer_collect_source_props() {
			g_zxfer_source_pvs_raw="compression=off=local,casesensitivity=sensitive=local"
			g_zxfer_source_pvs_effective=$g_zxfer_source_pvs_raw
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
		zxfer_exists_destination() {
			printf '%s\n' 0
		}
		zxfer_ensure_destination_exists() {
			printf 'override=%s\n' "$3" >>"$LOG_FILE"
			printf 'creation=%s\n' "$4" >>"$LOG_FILE"
			return 0
		}
		zxfer_capture_backup_metadata_for_completed_transfer() {
			:
		}

		zxfer_transfer_properties "tank/src"
	)

	result=$(cat "$log")
	assertContains "Unsupported properties should be stripped from the apply plan when -U is active." \
		"$result" "override=compression=lz4=override"
	assertContains "Unsupported properties should be stripped from the missing-dataset create plan when -U is active." \
		"$result" "creation=compression=lz4=override"
	assertNotContains "Unsupported create-time properties should not survive in either property plan." \
		"$result" "casesensitivity=sensitive"
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

test_property_transfer_stage_helpers_preserve_stage_allocation_statuses() {
	set +e
	output=$(
		(
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="compression=lz4=local"
				g_zxfer_source_pvs_effective="compression=lz4=local"
			}
			zxfer_get_validated_source_dataset_create_metadata() {
				printf '%s\n%s\n' "filesystem" ""
			}
			zxfer_create_property_reconcile_stage_file() {
				return 74
			}
			zxfer_prepare_property_transfer_source_context "tank/src" >/dev/null
			printf 'source_status=%s\n' "$?"
		)
		(
			zxfer_create_property_reconcile_stage_file() {
				return 75
			}
			zxfer_collect_source_props "tank/src" "backup/dst" 0 "mock_zfs" >/dev/null
			printf 'collect_source_status=%s\n' "$?"
		)
		(
			zxfer_create_property_reconcile_stage_file() {
				return 76
			}
			zxfer_collect_property_transfer_destination_context "creation" "" >/dev/null
			printf 'destination_status=%s\n' "$?"
		)
		(
			zxfer_create_property_reconcile_stage_file() {
				return 77
			}
			zxfer_diff_property_transfer_changes 1 "" "" "creation" "" >/dev/null
			printf 'diff_status=%s\n' "$?"
		)
	)
	set -e

	assertContains "Source property context should preserve stage allocation failures." \
		"$output" "source_status=74"
	assertContains "Source property collection should preserve stage allocation failures." \
		"$output" "collect_source_status=75"
	assertContains "Destination property context should preserve stage allocation failures." \
		"$output" "destination_status=76"
	assertContains "Property diff context should preserve stage allocation failures." \
		"$output" "diff_status=77"
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

test_transfer_properties_rethrows_source_required_property_staged_readback_failures() {
	set +e
	output=$(
		(
			l_tmp_path="$g_zxfer_run_tmp_root/transfer_source_required_readback.tmp"
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
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$l_tmp_path
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_ensure_required_properties_present() {
				printf '%s\n' "$2"
			}
			zxfer_read_property_reconcile_stage_file() {
				return 41
			}
			g_actual_dest="backup/dst"
			g_initial_source="tank/src"
			g_recursive_dest_list=""
			zxfer_transfer_properties "tank/src"
			l_status=$?
			printf 'status=%s\n' "$l_status"
			if [ -e "$l_tmp_path" ]; then
				printf 'tmp_exists=yes\n'
			else
				printf 'tmp_exists=no\n'
			fi
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Property transfer should preserve source required-property staged readback failures." \
		41 "$status"
	assertContains "Source required-property staged readback failures should clean the staged temp file." \
		"$output" "tmp_exists=no"
}

test_transfer_properties_rethrows_source_required_property_failure_readback_failures() {
	set +e
	output=$(
		(
			l_tmp_path="$g_zxfer_run_tmp_root/transfer_source_required_failure_readback.tmp"
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
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$l_tmp_path
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_ensure_required_properties_present() {
				printf '%s\n' "source required property failure"
				return 1
			}
			zxfer_read_property_reconcile_stage_file() {
				return 48
			}
			g_actual_dest="backup/dst"
			g_initial_source="tank/src"
			g_recursive_dest_list=""
			zxfer_transfer_properties "tank/src"
			l_status=$?
			printf 'status=%s\n' "$l_status"
			if [ -e "$l_tmp_path" ]; then
				printf 'tmp_exists=yes\n'
			else
				printf 'tmp_exists=no\n'
			fi
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Property transfer should preserve source required-property failure staged readback failures." \
		48 "$status"
	assertContains "Source required-property failure staged readback failures should clean the staged temp file." \
		"$output" "tmp_exists=no"
}

test_transfer_properties_rethrows_effective_source_required_property_failure_readback_failures() {
	set +e
	output=$(
		(
			l_tmp_path="$g_zxfer_run_tmp_root/transfer_effective_source_required_failure_readback.tmp"
			required_call_count=0
			read_call_count=0
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
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$l_tmp_path
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_ensure_required_properties_present() {
				required_call_count=$((required_call_count + 1))
				if [ "$required_call_count" -eq 1 ]; then
					printf '%s\n' "$2"
					return 0
				fi
				printf '%s\n' "effective source required property failure"
				return 1
			}
			zxfer_read_property_reconcile_stage_file() {
				read_call_count=$((read_call_count + 1))
				if [ "$read_call_count" -eq 2 ]; then
					return 49
				fi
				g_zxfer_property_stage_file_read_result=$(cat "$1")
				return 0
			}
			g_actual_dest="backup/dst"
			g_initial_source="tank/src"
			g_recursive_dest_list=""
			zxfer_transfer_properties "tank/src"
			l_status=$?
			printf 'status=%s\n' "$l_status"
			if [ -e "$l_tmp_path" ]; then
				printf 'tmp_exists=yes\n'
			else
				printf 'tmp_exists=no\n'
			fi
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Property transfer should preserve effective source required-property failure staged readback failures." \
		49 "$status"
	assertContains "Effective source required-property failure staged readback failures should clean the staged temp file." \
		"$output" "tmp_exists=no"
}

test_transfer_properties_rethrows_effective_source_required_property_staged_readback_failures() {
	set +e
	output=$(
		(
			l_tmp_path="$g_zxfer_run_tmp_root/transfer_effective_source_required_readback.tmp"
			read_call_count=0
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
			zxfer_get_temp_file() {
				g_zxfer_temp_file_result=$l_tmp_path
				: >"$g_zxfer_temp_file_result"
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_ensure_required_properties_present() {
				printf '%s\n' "$2"
			}
			zxfer_read_property_reconcile_stage_file() {
				read_call_count=$((read_call_count + 1))
				if [ "$read_call_count" -eq 2 ]; then
					return 50
				fi
				g_zxfer_property_stage_file_read_result=$(cat "$1")
				return 0
			}
			g_actual_dest="backup/dst"
			g_initial_source="tank/src"
			g_recursive_dest_list=""
			zxfer_transfer_properties "tank/src"
			l_status=$?
			printf 'status=%s\n' "$l_status"
			if [ -e "$l_tmp_path" ]; then
				printf 'tmp_exists=yes\n'
			else
				printf 'tmp_exists=no\n'
			fi
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Property transfer should preserve effective source required-property staged readback failures." \
		50 "$status"
	assertContains "Effective source required-property staged readback failures should clean the staged temp file." \
		"$output" "tmp_exists=no"
}

test_transfer_properties_rethrows_destination_property_staged_readback_failures() {
	set +e
	output=$(
		(
			temp_call_count=0
			dest_tmp_path="$g_zxfer_run_tmp_root/transfer_destination_readback.tmp"
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
			zxfer_get_temp_file() {
				temp_call_count=$((temp_call_count + 1))
				if [ "$temp_call_count" -eq 1 ]; then
					g_zxfer_temp_file_result="$g_zxfer_run_tmp_root/transfer_destination_source_required.tmp"
					: >"$g_zxfer_temp_file_result"
				elif [ "$temp_call_count" -eq 2 ]; then
					g_zxfer_temp_file_result=$dest_tmp_path
					: >"$g_zxfer_temp_file_result"
				else
					g_zxfer_temp_file_result="$g_zxfer_run_tmp_root/transfer_destination_unexpected_$temp_call_count.tmp"
					: >"$g_zxfer_temp_file_result"
				fi
				printf '%s\n' "$g_zxfer_temp_file_result"
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
				printf '%s\n' "compression=off=local"
			}
			zxfer_read_property_reconcile_stage_file() {
				if [ "$1" = "$dest_tmp_path" ]; then
					return 42
				fi
				g_zxfer_property_stage_file_read_result=$(cat "$1")
				return 0
			}
			g_recursive_dest_list="backup/dst"
			g_actual_dest="backup/dst"
			g_initial_source="tank/src"
			zxfer_transfer_properties "tank/src"
			l_status=$?
			printf 'status=%s\n' "$l_status"
			if [ -e "$dest_tmp_path" ]; then
				printf 'tmp_exists=yes\n'
			else
				printf 'tmp_exists=no\n'
			fi
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Property transfer should preserve destination property staged readback failures." \
		42 "$status"
	assertContains "Destination property staged readback failures should clean the staged temp file." \
		"$output" "tmp_exists=no"
}

test_transfer_properties_rethrows_destination_required_property_failure_readback_failures() {
	set +e
	output=$(
		(
			temp_call_count=0
			dest_tmp_path="$g_zxfer_run_tmp_root/transfer_destination_required_failure_readback.tmp"
			read_call_count=0
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="compression=lz4=local,casesensitivity=sensitive=local"
				g_zxfer_source_pvs_effective="$g_zxfer_source_pvs_raw"
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			zxfer_get_temp_file() {
				temp_call_count=$((temp_call_count + 1))
				if [ "$temp_call_count" -eq 1 ]; then
					g_zxfer_temp_file_result="$g_zxfer_run_tmp_root/transfer_destination_required_failure_source.tmp"
					: >"$g_zxfer_temp_file_result"
				elif [ "$temp_call_count" -eq 2 ]; then
					g_zxfer_temp_file_result=$dest_tmp_path
					: >"$g_zxfer_temp_file_result"
				else
					g_zxfer_temp_file_result="$g_zxfer_run_tmp_root/transfer_destination_required_failure_unexpected_$temp_call_count.tmp"
					: >"$g_zxfer_temp_file_result"
				fi
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_ensure_required_properties_present() {
				if [ "$1" = "tank/src" ]; then
					printf '%s\n' "$2"
					return 0
				fi
				printf '%s\n' "destination required property failure"
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
				printf '%s\n' "compression=off=local"
			}
			zxfer_read_property_reconcile_stage_file() {
				read_call_count=$((read_call_count + 1))
				if [ "$1" = "$dest_tmp_path" ] && [ "$read_call_count" -eq 4 ]; then
					return 43
				fi
				g_zxfer_property_stage_file_read_result=$(cat "$1")
				return 0
			}
			g_recursive_dest_list="backup/dst"
			g_actual_dest="backup/dst"
			g_initial_source="tank/src"
			zxfer_transfer_properties "tank/src"
			l_status=$?
			printf 'status=%s\n' "$l_status"
			if [ -e "$dest_tmp_path" ]; then
				printf 'tmp_exists=yes\n'
			else
				printf 'tmp_exists=no\n'
			fi
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Property transfer should preserve destination required-property failure staged readback failures." \
		43 "$status"
	assertContains "Destination required-property failure staged readback failures should clean the staged temp file." \
		"$output" "tmp_exists=no"
}

test_transfer_properties_rethrows_destination_required_property_success_readback_failures() {
	set +e
	output=$(
		(
			temp_call_count=0
			dest_tmp_path="$g_zxfer_run_tmp_root/transfer_destination_required_success_readback.tmp"
			read_call_count=0
			zxfer_collect_source_props() {
				g_zxfer_source_pvs_raw="compression=lz4=local,casesensitivity=sensitive=local"
				g_zxfer_source_pvs_effective="$g_zxfer_source_pvs_raw"
			}
			zxfer_run_source_zfs_cmd() {
				if [ "$4" = "type" ]; then
					printf '%s\n' "filesystem"
				else
					printf '%s\n' "-"
				fi
			}
			zxfer_get_temp_file() {
				temp_call_count=$((temp_call_count + 1))
				if [ "$temp_call_count" -eq 1 ]; then
					g_zxfer_temp_file_result="$g_zxfer_run_tmp_root/transfer_destination_required_success_source.tmp"
					: >"$g_zxfer_temp_file_result"
				elif [ "$temp_call_count" -eq 2 ]; then
					g_zxfer_temp_file_result=$dest_tmp_path
					: >"$g_zxfer_temp_file_result"
				else
					g_zxfer_temp_file_result="$g_zxfer_run_tmp_root/transfer_destination_required_success_unexpected_$temp_call_count.tmp"
					: >"$g_zxfer_temp_file_result"
				fi
				printf '%s\n' "$g_zxfer_temp_file_result"
			}
			zxfer_ensure_required_properties_present() {
				printf '%s\n' "$2"
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
				printf '%s\n' "compression=off=local,casesensitivity=sensitive=local"
			}
			zxfer_read_property_reconcile_stage_file() {
				read_call_count=$((read_call_count + 1))
				if [ "$1" = "$dest_tmp_path" ] && [ "$read_call_count" -eq 4 ]; then
					return 44
				fi
				g_zxfer_property_stage_file_read_result=$(cat "$1")
				return 0
			}
			g_recursive_dest_list="backup/dst"
			g_actual_dest="backup/dst"
			g_initial_source="tank/src"
			zxfer_transfer_properties "tank/src"
			l_status=$?
			printf 'status=%s\n' "$l_status"
			if [ -e "$dest_tmp_path" ]; then
				printf 'tmp_exists=yes\n'
			else
				printf 'tmp_exists=no\n'
			fi
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Property transfer should preserve destination required-property success staged readback failures." \
		44 "$status"
	assertContains "Destination required-property success staged readback failures should clean the staged temp file." \
		"$output" "tmp_exists=no"
}

test_transfer_properties_reports_generic_diff_failures() {
	set +e
	output=$(
		(
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
				printf '%s\n' "compression=off=local"
			}
			zxfer_diff_properties() {
				printf '%s\n' "diff failure"
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

	assertEquals "Property transfer should fail closed when property diffing fails." \
		1 "$status"
	assertEquals "Property transfer should preserve the destination-specific diff failure message." \
		"Failed to calculate property reconciliation changes for destination [backup/dst]." "$output"
}

test_transfer_properties_rethrows_diff_readback_failures() {
	set +e
	output=$(
		(
			temp_call_count=0
			diff_tmp_path="$g_zxfer_run_tmp_root/transfer_diff_readback.tmp"
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
			zxfer_get_temp_file() {
				temp_call_count=$((temp_call_count + 1))
				if [ "$temp_call_count" -eq 1 ]; then
					g_zxfer_temp_file_result="$g_zxfer_run_tmp_root/transfer_diff_source_required.tmp"
					: >"$g_zxfer_temp_file_result"
				elif [ "$temp_call_count" -eq 2 ]; then
					g_zxfer_temp_file_result="$g_zxfer_run_tmp_root/transfer_diff_destination.tmp"
					: >"$g_zxfer_temp_file_result"
				elif [ "$temp_call_count" -eq 3 ]; then
					g_zxfer_temp_file_result=$diff_tmp_path
					: >"$g_zxfer_temp_file_result"
				else
					g_zxfer_temp_file_result="$g_zxfer_run_tmp_root/transfer_diff_unexpected_$temp_call_count.tmp"
					: >"$g_zxfer_temp_file_result"
				fi
				printf '%s\n' "$g_zxfer_temp_file_result"
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
				printf '%s\n' "compression=off=local"
			}
			zxfer_diff_properties() {
				printf '%s\n%s\n%s\n' "compression=lz4 source=local" "-" "-"
			}
			zxfer_read_property_reconcile_stage_file() {
				if [ "$1" = "$diff_tmp_path" ]; then
					return 45
				fi
				g_zxfer_property_stage_file_read_result=$(cat "$1")
				return 0
			}
			g_recursive_dest_list="backup/dst"
			g_actual_dest="backup/dst"
			g_initial_source="tank/src"
			zxfer_transfer_properties "tank/src"
			l_status=$?
			printf 'status=%s\n' "$l_status"
			if [ -e "$diff_tmp_path" ]; then
				printf 'tmp_exists=yes\n'
			else
				printf 'tmp_exists=no\n'
			fi
			exit "$l_status"
		)
	)
	status=$?

	assertEquals "Property transfer should preserve diff staged readback failures." \
		45 "$status"
	assertContains "Diff staged readback failures should clean the staged temp file." \
		"$output" "tmp_exists=no"
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
		g_recursive_dest_list="backup/vol"
		zxfer_transfer_properties "tank/vol"
	)

	assertEquals "Volume transfers should not probe filesystem-only creation-time properties before creation." \
		"ensure-required tank/vol compression=lz4=local
ensure-required tank/vol compression=lz4=local
ensure volume 8M backup/vol" "$(cat "$log")"
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
	zxfer_exists_destination() {
		printf '%s\n' 0
	}
	zxfer_append_backup_metadata_record() {
		printf 'backup_append %s %s\n' "$1" "$2" >>"$append_log"
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
	unset -f zxfer_exists_destination
	unset -f zxfer_append_backup_metadata_record
	unset -f zxfer_write_backup_properties
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

	assertContains "Writable-mode transfers should validate overrides after forcing readonly=off." \
		"$(cat "$log")" "validate readonly=off"
	assertContains "Writable-mode backup capture should preserve the raw source properties for restore mode through the owner helper." \
		"$(cat "$append_log")" "backup_append tank/src readonly=on=local,compression=lz4=local"
	assertNotContains "Writable-mode property reconciliation should not flush backup metadata directly." \
		"$(cat "$append_log")" "unexpected backup_write"
	assertEquals "Writable-mode backup accumulation state should remain helper-owned." \
		"helper-owned" "$g_backup_file_contents"
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
	zxfer_exists_destination() {
		printf '%s\n' 0
	}
	zxfer_get_forwarded_backup_properties_for_source() {
		g_forwarded_backup_properties="compression=lz4=local"
		printf '%s\n' "$g_forwarded_backup_properties"
	}
	zxfer_append_backup_metadata_record() {
		printf 'backup_append %s %s\n' "$1" "$2" >>"$append_log"
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
	unset -f zxfer_exists_destination
	unset -f zxfer_get_forwarded_backup_properties_for_source
	unset -f zxfer_append_backup_metadata_record
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

	assertContains "Chained backup capture should prefer forwarded provenance from the intermediate backup metadata over the intermediate dataset's live properties." \
		"$(cat "$append_log")" "backup_append backup/intermediate/src compression=lz4=local"
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
	zxfer_exists_destination() {
		printf '%s\n' 0
	}
	zxfer_append_backup_metadata_record() {
		printf 'unexpected %s %s\n' "$1" "$2" >>"$append_log"
	}

	zxfer_transfer_properties "tank/src" 1

	unset -f zxfer_collect_source_props
	unset -f zxfer_run_source_zfs_cmd
	unset -f zxfer_validate_override_properties
	unset -f zxfer_derive_override_lists
	unset -f zxfer_sanitize_property_list
	unset -f zxfer_strip_unsupported_properties
	unset -f zxfer_ensure_destination_exists
	unset -f zxfer_exists_destination
	unset -f zxfer_append_backup_metadata_record
	zxfer_source_runtime_modules_through "zxfer_property_reconcile.sh"

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

test_transfer_properties_reports_adjust_child_inherit_failures_for_existing_children() {
	set +e
	output=$(
		(
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
			zxfer_ensure_destination_exists() {
				return 1
			}
			zxfer_collect_destination_props() {
				printf '%s\n' "compression=lz4=local"
			}
			zxfer_diff_properties() {
				printf '\n'
				printf 'compression=lz4\n'
				printf 'compression=lz4\n'
			}
			zxfer_adjust_child_inherit_to_match_parent() {
				return 1
			}
			zxfer_throw_error() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_transfer_properties "tank/src/child"
		) 2>&1
	)
	status=$?

	assertEquals "Existing child property reconciliation should fail closed when child inherit adjustment fails." \
		1 "$status"
	assertEquals "Existing child property reconciliation should preserve the destination-specific adjust failure." \
		"Failed to reconcile inherited child properties for destination [backup/dst/child]." "$output"
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

test_transfer_properties_inherits_matching_recursive_overrides_for_existing_children() {
	log="$TEST_TMPDIR/transfer_child_override_inherit.log"
	: >"$log"
	(
		g_initial_source="tank/src"
		g_actual_dest="backup/dst/child"
		g_recursive_dest_list="backup/dst
backup/dst/child"
		g_option_P_transfer_property=0
		g_option_o_override_property="quota=32M,checksum=sha256"

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
		zxfer_ensure_required_properties_present() {
			printf '%s\n' "$2"
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
		zxfer_collect_destination_props() {
			case "$1" in
			backup/dst/child) printf '%s\n' "quota=32M=local,checksum=sha256=local" ;;
			backup/dst) printf '%s\n' "quota=32M=local,checksum=sha256=local" ;;
			*)
				printf '%s\n' "unexpected dataset $1"
				return 1
				;;
			esac
		}
		zxfer_apply_property_changes() {
			printf 'apply %s %s %s %s %s\n' "$1" "$2" "$3" "$4" "$5" >>"$log"
		}
		zxfer_capture_backup_metadata_for_completed_transfer() {
			:
		}

		zxfer_transfer_properties "tank/src/child"
	) >"$log" 2>&1

	assertContains "Existing descendants should inherit matching recursive -o overrides instead of keeping a local child property." \
		"$(cat "$log")" "apply backup/dst/child 0   checksum=sha256"
	assertNotContains "Existing descendants should not inherit non-inheritable recursive -o overrides." \
		"$(cat "$log")" "quota=32M"
}

test_transfer_properties_inherits_changed_recursive_overrides_for_existing_children() {
	log="$TEST_TMPDIR/transfer_child_override_changed_inherit.log"
	: >"$log"
	(
		g_initial_source="tank/src"
		g_actual_dest="backup/dst/child"
		g_recursive_dest_list="backup/dst
backup/dst/child"
		g_option_P_transfer_property=1
		g_option_o_override_property="quota=32M,checksum=sha256"
		g_option_I_ignore_properties="mountpoint,compression"

		zxfer_collect_source_props() {
			g_zxfer_source_pvs_raw="quota=none=default,checksum=sha256=inherited,compression=lz4=inherited,atime=off=local"
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
		zxfer_exists_destination() {
			printf '1\n'
		}
		zxfer_collect_destination_props() {
			case "$1" in
			backup/dst/child) printf '%s\n' "quota=none=default,checksum=fletcher4=local,compression=off=local,atime=on=local" ;;
			backup/dst) printf '%s\n' "quota=32M=local,checksum=sha256=local,compression=off=local,atime=off=local" ;;
			*)
				printf '%s\n' "unexpected dataset $1"
				return 1
				;;
			esac
		}
		zxfer_apply_property_changes() {
			{
				printf 'apply destination=%s\n' "$1"
				printf 'apply is_initial=%s\n' "$2"
				printf 'apply initial=%s\n' "$3"
				printf 'apply child=%s\n' "$4"
				printf 'apply inherit=%s\n' "$5"
			} >>"$log"
		}
		zxfer_capture_backup_metadata_for_completed_transfer() {
			:
		}

		zxfer_transfer_properties "tank/src/child"
	) >"$log" 2>&1

	assertContains "Changed inheritable recursive -o overrides should inherit from a matching parent on existing descendants." \
		"$(cat "$log")" "apply inherit=checksum=sha256"
	assertContains "Local source and non-inheritable override properties should remain in the child set list." \
		"$(cat "$log")" "apply child=quota=32M,atime=off"
	assertNotContains "Changed inheritable recursive -o overrides should not stay in the local child set list when the parent matches." \
		"$(cat "$log")" "apply child=quota=32M,checksum=sha256"
}

test_transfer_properties_uses_freebsd_readonly_properties_without_mutating_global_state() {
	log="$TEST_TMPDIR/transfer_freebsd_readonly.log"
	: >"$log"
	g_destination_operating_system="FreeBSD"
	g_source_operating_system="Linux"
	ZXFER_BASE_READONLY_PROPERTIES="readonly"
	ZXFER_FREEBSD_READONLY_PROPERTIES="aclmode"
	g_initial_source="tank/src"
	g_actual_dest="backup/dst"
	g_recursive_dest_list="backup/dst"
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
		"readonly" "$ZXFER_BASE_READONLY_PROPERTIES"
	assertEquals "Repeated transfers should reuse the same effective FreeBSD readonly list instead of appending duplicates." \
		"readonly,aclmode
readonly,aclmode
readonly,aclmode
readonly,aclmode" "$(cat "$log")"
}

test_transfer_properties_uses_shared_sunos_readonly_properties_without_extra_delta() {
	log="$TEST_TMPDIR/transfer_sunos_readonly.log"
	: >"$log"
	g_destination_operating_system="SunOS"
	g_source_operating_system="FreeBSD"
	ZXFER_BASE_READONLY_PROPERTIES="readonly"
	g_initial_source="tank/src"
	g_actual_dest="backup/dst"
	g_recursive_dest_list="backup/dst"
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

	assertEquals "SunOS transfers should not mutate the global base readonly list." \
		"readonly" "$ZXFER_BASE_READONLY_PROPERTIES"
	assertEquals "Repeated FreeBSD-to-SunOS transfers should use the shared readonly list without appending an extra SunOS delta." \
		"readonly
readonly
readonly
readonly" "$(cat "$log")"
}

# Register this fragment's tests explicitly so unfiltered shunit2 execution
# cannot depend on source scanning or evaluation.
zxfer_test_add_property_transfer_tests() {
	suite_addTest test_transfer_properties_fails_when_source_property_collection_fails
	suite_addTest test_transfer_properties_creates_destination_and_records_backup
	suite_addTest test_transfer_properties_does_not_capture_backup_metadata_before_success
	suite_addTest test_transfer_properties_rethrows_override_derivation_failures
	suite_addTest test_transfer_properties_selects_unsupported_properties_for_current_dataset_type
	suite_addTest test_transfer_properties_diffs_existing_destinations_and_applies_changes
	suite_addTest test_transfer_properties_queries_missing_must_create_properties_before_diffing
	suite_addTest test_transfer_properties_backfills_required_create_props_after_restore_replacement
	suite_addTest test_transfer_properties_preserves_required_create_props_when_transfer_all_disabled
	suite_addTest test_transfer_properties_preserves_child_override_absent_from_source_on_create
	suite_addTest test_transfer_properties_strips_unsupported_creation_props_when_requested
	suite_addTest test_transfer_properties_propagates_must_create_diff_failures
	suite_addTest test_transfer_properties_rethrows_tempfile_allocation_failures
	suite_addTest test_property_transfer_stage_helpers_preserve_stage_allocation_statuses
	suite_addTest test_transfer_properties_fails_when_source_required_property_probe_fails
	suite_addTest test_transfer_properties_fails_when_effective_source_required_property_probe_fails
	suite_addTest test_transfer_properties_rethrows_source_required_property_staged_readback_failures
	suite_addTest test_transfer_properties_rethrows_source_required_property_failure_readback_failures
	suite_addTest test_transfer_properties_rethrows_effective_source_required_property_failure_readback_failures
	suite_addTest test_transfer_properties_rethrows_effective_source_required_property_staged_readback_failures
	suite_addTest test_transfer_properties_rethrows_destination_property_staged_readback_failures
	suite_addTest test_transfer_properties_rethrows_destination_required_property_failure_readback_failures
	suite_addTest test_transfer_properties_rethrows_destination_required_property_success_readback_failures
	suite_addTest test_transfer_properties_reports_generic_diff_failures
	suite_addTest test_transfer_properties_rethrows_diff_readback_failures
	suite_addTest test_transfer_properties_fails_when_destination_required_property_probe_fails
	suite_addTest test_transfer_properties_fails_when_destination_property_collection_fails
	suite_addTest test_transfer_properties_skips_filesystem_only_required_property_probes_for_volumes
	suite_addTest test_transfer_properties_fails_when_source_type_probe_fails
	suite_addTest test_transfer_properties_fails_when_source_volume_size_probe_fails
	suite_addTest test_transfer_properties_fails_when_source_volume_size_is_empty
	suite_addTest test_transfer_properties_forces_readonly_overrides_in_current_shell
	suite_addTest test_transfer_properties_preserves_escaped_comma_override_end_to_end
	suite_addTest test_transfer_properties_prefers_forwarded_backup_provenance_for_chained_backup_capture
	suite_addTest test_transfer_properties_skip_backup_capture_preserves_existing_backup_contents
	suite_addTest test_transfer_properties_adjusts_child_inherit_lists_for_existing_children
	suite_addTest test_transfer_properties_reports_adjust_child_inherit_failures_for_existing_children
	suite_addTest test_transfer_properties_adjusts_set_only_inherited_child_properties_for_existing_children
	suite_addTest test_transfer_properties_inherits_matching_recursive_overrides_for_existing_children
	suite_addTest test_transfer_properties_inherits_changed_recursive_overrides_for_existing_children
	suite_addTest test_transfer_properties_uses_freebsd_readonly_properties_without_mutating_global_state
	suite_addTest test_transfer_properties_uses_shared_sunos_readonly_properties_without_extra_delta
}
