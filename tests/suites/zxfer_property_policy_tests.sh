#!/bin/sh
# Property policy fragment: source collection and -e restore, override
# derivation and -o validation, list sanitizing, create metadata, and the -U
# unsupported-property scan.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

################################################################################
# SOURCE COLLECTION
################################################################################

test_collect_source_props_publishes_raw_and_effective_lists() {
	(
		zxfer_load_normalized_dataset_properties() {
			printf '%s\n' "$*" >>"$TEST_TMPDIR/collect_side.log"
			g_zxfer_normalized_dataset_properties="compression=lz4=local,readonly=on=local"
		}
		zxfer_collect_source_props "tank/src" "backup/dst"
		printf 'raw=%s\neffective=%s\n' "$g_zxfer_source_pvs_raw" "$g_zxfer_source_pvs_effective"
	) >"$TEST_TMPDIR/collect_plain.out"
	assertEquals "raw=compression=lz4=local,readonly=on=local
effective=compression=lz4=local,readonly=on=local" "$(cat "$TEST_TMPDIR/collect_plain.out")"
	assertEquals "Source properties are read through the source side." \
		"tank/src source" "$(cat "$TEST_TMPDIR/collect_side.log")"
}

test_collect_source_props_propagates_lookup_failures_with_diagnostic() {
	(
		zxfer_load_normalized_dataset_properties() {
			g_zxfer_property_error_result="cannot open tank/src: permission denied"
			return 3
		}
		zxfer_collect_source_props "tank/src" "backup/dst"
		printf 'status=%s raw=<%s> error=%s\n' "$?" "$g_zxfer_source_pvs_raw" "$g_zxfer_property_error_result"
	) >"$TEST_TMPDIR/collect_failure.out"
	assertEquals "status=3 raw=<> error=cannot open tank/src: permission denied" \
		"$(cat "$TEST_TMPDIR/collect_failure.out")"
}

test_collect_source_props_uses_backup_restore() {
	(
		zxfer_load_normalized_dataset_properties() {
			g_zxfer_normalized_dataset_properties="compression=lz4=local,readonly=on=local"
		}
		g_option_e_restore_property_mode=1
		ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
		ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
		g_restored_backup_file_contents=$(zxfer_test_render_current_backup_metadata_contents \
			"$(zxfer_test_backup_metadata_row "." "readonly=on=local,compression=lz4=local")")
		zxfer_collect_source_props "tank/src" "backup/dst"
		printf 'raw=%s\neffective=%s\n' "$g_zxfer_source_pvs_raw" "$g_zxfer_source_pvs_effective"
	) >"$TEST_TMPDIR/collect_restore.out"
	assertEquals "raw=compression=lz4=local,readonly=on=local
effective=readonly=on=local,compression=lz4=local" "$(cat "$TEST_TMPDIR/collect_restore.out")"
}

test_collect_source_props_fails_when_backup_entry_missing() {
	set +e
	output=$(
		(
			zxfer_load_normalized_dataset_properties() {
				g_zxfer_normalized_dataset_properties="compression=lz4=local"
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
			ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
			g_restored_backup_file_contents=$(zxfer_test_render_current_backup_metadata_contents \
				"$(zxfer_test_backup_metadata_row "other" "compression=lz4=local")")
			zxfer_collect_source_props "tank/src" "backup/dst"
		)
	)
	status=$?
	assertEquals 2 "$status"
	assertEquals "Can't find the properties for the filesystem tank/src and destination backup/dst" "$output"
}

test_collect_source_props_restore_mode_requires_restored_contents() {
	set +e
	output=$(
		(
			zxfer_load_normalized_dataset_properties() {
				g_zxfer_normalized_dataset_properties="compression=lz4=local"
			}
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			g_option_e_restore_property_mode=1
			g_restored_backup_file_contents=""
			zxfer_collect_source_props "tank/src" "backup/dst"
		)
	)
	status=$?
	assertEquals 2 "$status"
	assertEquals "Can't find the properties for the filesystem tank/src and destination backup/dst" "$output"
}

################################################################################
# OVERRIDE VALIDATION / DERIVATION
################################################################################

# Purpose: Derive the override lists with -o validation on and print the
# status, then the usage error or the override list.
# Usage: zxfer_property_test_validate_override SOURCE_PVS OVERRIDE_OPTIONS
zxfer_property_test_validate_override() {
	(
		zxfer_throw_usage_error() {
			printf 'usage: %s\n' "$1"
			exit 2
		}
		zxfer_derive_override_lists "$1" "$2" 0 filesystem "" "" "" 1
		printf 'override: <%s>\n' "$g_zxfer_override_pvs_result"
	)
	printf 'status: %s\n' "$?"
}

test_derive_override_lists_validates_override_names_against_the_source() {
	assertEquals "An empty -o list always validates." "override: <>
status: 0" "$(zxfer_property_test_validate_override "compression=lz4=local" "")"
	assertEquals "override: <compression=gzip=override>
status: 0" "$(zxfer_property_test_validate_override "compression=lz4=local" "compression=gzip")"
	assertEquals "usage: Missing source property for -o override: copies.
status: 2" "$(zxfer_property_test_validate_override "compression=lz4=local" "compression=gzip,copies=2")"
}

test_derive_override_lists_skips_validation_unless_requested() {
	zxfer_derive_override_lists "compression=lz4=local" "copies=2" 0 filesystem
	assertEquals "Child datasets derive without validating -o names." \
		"copies=2=override" "$g_zxfer_override_pvs_result"
}

test_derive_override_lists_validates_escaped_commas_and_reports_syntax_first() {
	assertEquals "override: <user:note=a%2Cb=override,compression=lz4=override>
status: 0" "$(zxfer_property_test_validate_override \
		"user:note=x=local,compression=lz4=local" 'user:note=a\,b,compression=lz4')"
	assertEquals "usage: Invalid option property - check -o list for syntax errors.
status: 2" "$(zxfer_property_test_validate_override "compression=lz4=local" "compression")"
}

test_derive_override_lists_validates_source_lists_with_backslashes_literally() {
	assertEquals "A backslash-zero value earlier in the source list must not hide later properties." \
		"override: <compression=zstd=override>
status: 0" "$(zxfer_property_test_validate_override \
			'com.x:path=C:\0data=local,compression=lz4=local' "compression=zstd")"
	assertEquals "An octal comma escape in a source value must not invent a property." \
		"usage: Missing source property for -o override: bogusprop.
status: 2" "$(zxfer_property_test_validate_override \
			'com.x:path=x\054bogusprop=local' "bogusprop=1")"
}

zxfer_property_test_derive() {
	zxfer_derive_override_lists "$@"
	printf '%s\n%s\n' "$g_zxfer_override_pvs_result" "$g_zxfer_creation_pvs_result"
}

test_derive_override_lists_preserves_override_only_mode_order() {
	assertEquals "compression=lz4=override,quota=1G=override
compression=lz4=override,quota=1G=override" \
		"$(zxfer_property_test_derive "" "compression=lz4,quota=1G" 0 filesystem)"
}

test_derive_override_lists_preserves_required_create_props_when_transfer_all_disabled() {
	assertEquals "compression=lz4=override,casesensitivity=sensitive=local,normalization=formD=local,utf8only=on=local
compression=lz4=override,casesensitivity=sensitive=local,normalization=formD=local,utf8only=on=local" \
		"$(zxfer_property_test_derive \
			"compression=off=local,casesensitivity=sensitive=local,normalization=formD=local,utf8only=on=local,quota=1G=local" \
			"compression=lz4" 0 filesystem)"
}

test_derive_override_lists_uses_required_create_override_for_creation() {
	assertEquals "casesensitivity=insensitive=override
casesensitivity=insensitive=override" \
		"$(zxfer_property_test_derive "casesensitivity=sensitive=local,compression=off=local" \
			"casesensitivity=insensitive" 0 filesystem)"
}

test_derive_override_lists_uses_explicit_override_for_inherited_creation() {
	assertEquals "An override of an inherited source property is applied but is not a creation-time property." \
		"compression=off=local,atime=off=override
compression=off=local" \
		"$(zxfer_property_test_derive "compression=off=local,atime=on=inherited" "atime=off" 1 filesystem)"
}

test_derive_override_lists_prefers_first_matching_override_when_transferring_all_properties() {
	assertEquals "compression=lz4=override
compression=lz4=override" \
		"$(zxfer_property_test_derive "compression=off=local" "compression=lz4,compression=gzip" 1 filesystem)"
}

test_derive_override_lists_transfer_all_keeps_sources_and_volume_refreservation() {
	assertEquals "compression=lz4=local,quota=8G=override,refreservation=4G=received
compression=lz4=local,quota=8G=override,refreservation=4G=received" \
		"$(zxfer_property_test_derive "compression=lz4=local,quota=1G=local,refreservation=4G=received" \
			"quota=8G" 1 volume)"
}

test_derive_override_lists_escapes_override_values_and_literal_commas() {
	assertEquals "user:note=a%3Db%2Cc%3B%25=override
user:note=a%3Db%2Cc%3B%25=override" \
		"$(zxfer_property_test_derive "" 'user:note=a=b\,c;%' 0 filesystem)"
}

test_derive_override_lists_preserves_literal_backslashes_in_overrides_and_source_values() {
	assertEquals 'user:path=C:\new=override,user:other=D:\temp=local
user:path=C:\new=override,user:other=D:\temp=local' \
		"$(zxfer_property_test_derive 'user:path=x=local,user:other=D:\temp=local' 'user:path=C:\new' 1 filesystem)"
}

test_derive_override_lists_skips_volume_only_properties_for_filesystems() {
	assertEquals "compression=lz4=local
compression=lz4=local" \
		"$(zxfer_property_test_derive "compression=lz4=local,volblocksize=8192=-,volthreading=on=default" "" 1 filesystem)"
	assertEquals "volblocksize=8192=-" \
		"$(zxfer_property_test_derive "volblocksize=8192=-" "" 1 volume)"
}

test_derive_override_lists_filters_readonly_ignored_and_unsupported_entries_with_warnings() {
	(
		g_option_v_verbose=1
		zxfer_derive_override_lists \
			"mountpoint=/mnt=local,compression=lz4=local,atime=off=local,user:note=a%2Cb=local,checksum=sha256=local" \
			"mountpoint=/other" 1 filesystem "mountpoint,readonly" "atime" "user:note,checksum" \
			2>"$TEST_TMPDIR/derive_filter.err"
		printf '%s\n%s\n' "$g_zxfer_override_pvs_result" "$g_zxfer_creation_pvs_result"
	) >"$TEST_TMPDIR/derive_filter.out"
	assertEquals "An explicit override survives the readonly filter; -I and -U entries are dropped from both lists." \
		"mountpoint=/other=override,compression=lz4=local
mountpoint=/other=override,compression=lz4=local" "$(cat "$TEST_TMPDIR/derive_filter.out")"
	assertEquals "Unsupported entries warn once per list, apply list first." \
		"Destination does not support property user:note=a,b
Destination does not support property checksum=sha256
Destination does not support property user:note=a,b
Destination does not support property checksum=sha256" "$(cat "$TEST_TMPDIR/derive_filter.err")"
}

test_derive_override_lists_rejects_missing_assignment_separator() {
	set +e
	output=$(
		(
			zxfer_throw_usage_error() {
				printf '%s\n' "$1"
				exit 2
			}
			zxfer_derive_override_lists "compression=lz4=local" "compression" 1 filesystem
		)
	)
	status=$?
	assertEquals 2 "$status"
	assertEquals "Invalid option property - check -o list for syntax errors." "$output"
}

test_derive_override_lists_reports_awk_failures() {
	set +e
	output=$(
		(
			g_cmd_awk="$TEST_TMPDIR/missing-awk"
			zxfer_test_stub_throw_error_to_stdout
			zxfer_derive_override_lists "compression=lz4=local" "" 1 filesystem 2>/dev/null
		)
	)
	status=$?
	assertEquals 1 "$status"
	assertEquals "Failed to derive override property lists." "$output"
}

################################################################################
# SANITIZE
################################################################################

test_sanitize_property_list_returns_input_when_nothing_filters() {
	zxfer_sanitize_property_list "" "readonly" "atime"
	assertEquals "" "$g_zxfer_sanitized_property_list_result"
	(
		g_cmd_awk="$TEST_TMPDIR/missing-awk"
		zxfer_sanitize_property_list "compression=lz4=local" "" ""
		printf '%s\n' "$g_zxfer_sanitized_property_list_result"
	) >"$TEST_TMPDIR/sanitize_passthrough.out"
	assertEquals "An empty filter set must not spawn awk." \
		"compression=lz4=local" "$(cat "$TEST_TMPDIR/sanitize_passthrough.out")"
}

test_sanitize_property_list_removes_readonly_and_ignored_but_keeps_overrides() {
	zxfer_sanitize_property_list \
		"mountpoint=/mnt=local,readonly=on=override,compression=lz4=local,atime=off=local,quota=1G=override" \
		"readonly,mountpoint" "atime,quota"
	assertEquals "readonly=on=override,compression=lz4=local,quota=1G=override" \
		"$g_zxfer_sanitized_property_list_result"
}

test_sanitize_property_list_reports_awk_failures() {
	set +e
	output=$(
		(
			g_cmd_awk="$TEST_TMPDIR/missing-awk"
			zxfer_test_stub_throw_error_to_stdout
			zxfer_sanitize_property_list "compression=lz4=local" "readonly" "" 2>/dev/null
		)
	)
	status=$?
	assertEquals 1 "$status"
	assertEquals "Failed to filter unsupported destination properties." "$output"
}

################################################################################
# CREATE METADATA
################################################################################

test_get_validated_source_dataset_create_metadata_reads_type_from_the_property_list() {
	(
		zxfer_run_source_zfs_cmd() {
			printf 'unexpected zfs call\n' >&2
			exit 99
		}
		zxfer_get_validated_source_dataset_create_metadata "tank/src" "compression=lz4=local,type=filesystem=-"
		printf 'status=%s type=%s volsize=<%s>\n' "$?" "$g_zxfer_source_dataset_type_result" "$g_zxfer_source_volume_size_result"
	) >"$TEST_TMPDIR/metadata_fs.out"
	assertEquals "status=0 type=filesystem volsize=<>" "$(cat "$TEST_TMPDIR/metadata_fs.out")"
}

test_get_validated_source_dataset_create_metadata_reads_volume_size_from_the_property_list() {
	(
		zxfer_run_source_zfs_cmd() {
			printf 'unexpected zfs call\n' >&2
			exit 99
		}
		zxfer_get_validated_source_dataset_create_metadata "tank/vol" "type=volume=-,volsize=1073741824=local"
		printf 'status=%s type=%s volsize=%s\n' "$?" "$g_zxfer_source_dataset_type_result" "$g_zxfer_source_volume_size_result"
	) >"$TEST_TMPDIR/metadata_vol.out"
	assertEquals "status=0 type=volume volsize=1073741824" "$(cat "$TEST_TMPDIR/metadata_vol.out")"
}

test_get_validated_source_dataset_create_metadata_probes_live_when_the_list_lacks_values() {
	(
		zxfer_run_source_zfs_cmd() {
			printf '%s\n' "$*" >>"$TEST_TMPDIR/metadata_probe.log"
			case "$*" in
			"get -Hpo value type tank/vol") printf 'volume\n' ;;
			"get -Hpo value volsize tank/vol") printf '2147483648\n' ;;
			esac
		}
		zxfer_get_validated_source_dataset_create_metadata "tank/vol" "compression=lz4=local"
		printf 'type=%s volsize=%s\n' "$g_zxfer_source_dataset_type_result" "$g_zxfer_source_volume_size_result"
	) >"$TEST_TMPDIR/metadata_probe.out"
	assertEquals "type=volume volsize=2147483648" "$(cat "$TEST_TMPDIR/metadata_probe.out")"
	assertEquals "get -Hpo value type tank/vol
get -Hpo value volsize tank/vol" "$(cat "$TEST_TMPDIR/metadata_probe.log")"
}

test_get_validated_source_dataset_create_metadata_reports_probe_failures_and_invalid_types() {
	(
		zxfer_run_source_zfs_cmd() {
			printf 'permission denied\n'
			return 5
		}
		zxfer_get_validated_source_dataset_create_metadata "tank/src" ""
		printf 'status=%s error=%s\n' "$?" "$g_zxfer_property_error_result"
		zxfer_get_validated_source_dataset_create_metadata "tank/src" "type=snapshot=-"
		printf 'status=%s error=%s\n' "$?" "$g_zxfer_property_error_result"
		zxfer_get_validated_source_dataset_create_metadata "tank/vol" "type=volume=-,volsize=-=-"
		printf 'status=%s error=%s\n' "$?" "$g_zxfer_property_error_result"
		zxfer_get_validated_source_dataset_create_metadata "tank/vol" "type=volume=-"
		printf 'status=%s error=%s\n' "$?" "$g_zxfer_property_error_result"
	) >"$TEST_TMPDIR/metadata_failures.out"
	assertEquals "status=5 error=Failed to retrieve source dataset type for [tank/src]: permission denied
status=1 error=Invalid source dataset type for [tank/src]: snapshot
status=1 error=Failed to retrieve source zvol size for [tank/vol]: empty volsize
status=5 error=Failed to retrieve source zvol size for [tank/vol]: permission denied" \
		"$(cat "$TEST_TMPDIR/metadata_failures.out")"
}

################################################################################
# UNSUPPORTED-PROPERTY SCAN
################################################################################

# Fake source/destination zfs for the -U scan: tank/src (filesystem) and
# tank/src/vol (volume) map to backup/dst and backup/dst/vol; the source
# filesystem inventory carries overlay (unknown on the destination), a user
# property, and volmode (volume-only on this destination).
zxfer_property_test_fake_unsupported_scan() {
	l_fake_role=$1
	shift
	# Join argv with explicit spaces: "$*" would use the caller's IFS.
	l_fake_key=""
	for l_fake_arg in "$@"; do
		l_fake_key="$l_fake_key $l_fake_arg"
	done
	l_fake_key=${l_fake_key# }
	printf '%s\n' "$l_fake_key" >>"$PROBE_LOG"
	case "$l_fake_role $l_fake_key" in
	*" get -Hpo name,value type tank/src tank/src/vol")
		printf 'tank/src\tfilesystem\ntank/src/vol\tvolume\n'
		;;
	*" get -Hpo name,value type tank/src")
		printf 'tank/src\tfilesystem\n'
		;;
	*" get -Hpo property all tank/src")
		printf 'compression\nuser:note\nrecordsize\noverlay\nvolmode\n'
		;;
	*" get -Hpo property all tank/src/vol")
		printf 'compression\nvolmode\noverlay\n'
		;;
	*" get -Hpo value type backup/dst") printf 'filesystem\n' ;;
	*" get -Hpo value type backup/dst/vol") printf 'volume\n' ;;
	*" get -Hpo value type backup") printf 'filesystem\n' ;;
	*" get -Hpo property all backup/dst" | *" get -Hpo property all backup")
		printf 'compression\nrecordsize\n'
		;;
	*" get -Hpo property all backup/dst/vol")
		printf 'compression\nvolmode\n'
		;;
	*" get -Hpo property,value,source overlay "*)
		printf 'bad property list: invalid property overlay\n'
		return 1
		;;
	*" get -Hpo property,value,source volmode "*)
		printf 'property volmode does not apply to datasets of this type\n'
		return 1
		;;
	*)
		printf 'unexpected zfs command: %s\n' "$*" >&2
		return 1
		;;
	esac
}

# Destination fakes with one overlay probe answer overridden. Defined at top
# level: a case statement inside "$(...)" is not portable across shells.
zxfer_property_test_fake_overlay_probe_error() {
	case "$*" in
	"get -Hpo property,value,source overlay backup/dst")
		printf 'connection reset\n'
		return 4
		;;
	esac
	zxfer_property_test_fake_unsupported_scan destination "$@"
}

zxfer_property_test_fake_overlay_probe_blank() {
	case "$*" in
	"get -Hpo property,value,source overlay backup/dst") return 1 ;;
	esac
	zxfer_property_test_fake_unsupported_scan destination "$@"
}

# Purpose: Run the -U scan against the fakes in a subshell, with every mapped
# destination reported as existing (1) or missing (0), and write the
# resulting lists to unsupported_scan.out.
# Usage: zxfer_property_test_run_unsupported_scan 1|0
zxfer_property_test_run_unsupported_scan() {
	PROBE_LOG="$TEST_TMPDIR/unsupported_probe.log"
	: >"$PROBE_LOG"
	(
		UNSUPPORTED_SCAN_DEST_EXISTS=$1
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=$UNSUPPORTED_SCAN_DEST_EXISTS
		}
		zxfer_run_source_zfs_cmd() { zxfer_property_test_fake_unsupported_scan source "$@"; }
		zxfer_run_destination_zfs_cmd() { zxfer_property_test_fake_unsupported_scan destination "$@"; }
		zxfer_calculate_unsupported_properties
		printf 'fs=<%s> vol=<%s>\n' "$g_zxfer_unsupported_filesystem_properties" \
			"$g_zxfer_unsupported_volume_properties"
	) >"$TEST_TMPDIR/unsupported_scan.out"
}

test_calculate_unsupported_properties_confirms_only_inventory_differences_with_direct_probes() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"

	zxfer_property_test_run_unsupported_scan 1

	assertEquals "Only overlay is unknown; volmode does not apply to a same-type probe and user:note is never probed." \
		"fs=<overlay,volmode> vol=<>" "$(cat "$TEST_TMPDIR/unsupported_scan.out")"
	assertEquals "get -Hpo name,value type tank/src
get -Hpo property all tank/src
get -Hpo value type backup/dst
get -Hpo property all backup/dst
get -Hpo property,value,source overlay backup/dst
get -Hpo property,value,source volmode backup/dst" "$(cat "$PROBE_LOG")"
}

test_calculate_unsupported_properties_scans_each_dataset_type_against_a_matching_destination() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src
tank/src/vol"
	g_destination="backup/dst"

	zxfer_property_test_run_unsupported_scan 1

	assertEquals "fs=<overlay,volmode> vol=<overlay>" "$(cat "$TEST_TMPDIR/unsupported_scan.out")"
	assertContains "The volume inventory is compared against the existing destination volume." \
		"$(cat "$PROBE_LOG")" "get -Hpo property all backup/dst/vol"
	assertEquals "Volume-only properties present on the destination volume are never probed." \
		0 "$(grep -c 'volmode backup/dst/vol' "$PROBE_LOG")"
}

test_calculate_unsupported_properties_falls_back_to_destination_pool_when_mapped_destination_is_missing() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"

	zxfer_property_test_run_unsupported_scan 0

	assertEquals "fs=<overlay,volmode> vol=<>" "$(cat "$TEST_TMPDIR/unsupported_scan.out")"
	assertContains "$(cat "$PROBE_LOG")" "get -Hpo property all backup"
}

test_calculate_unsupported_properties_leaves_type_mismatched_inconclusive_probes_supported() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src/vol"
	g_destination="backup/dst"

	PROBE_LOG="$TEST_TMPDIR/unsupported_probe.log"
	: >"$PROBE_LOG"
	(
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=0; }
		zxfer_run_source_zfs_cmd() {
			case "$*" in
			"get -Hpo name,value type tank/src/vol") printf 'tank/src/vol\tvolume\n' ;;
			*) zxfer_property_test_fake_unsupported_scan source "$@" ;;
			esac
		}
		zxfer_run_destination_zfs_cmd() { zxfer_property_test_fake_unsupported_scan destination "$@"; }
		zxfer_calculate_unsupported_properties
		printf 'vol=<%s>\n' "$g_zxfer_unsupported_volume_properties"
	) >"$TEST_TMPDIR/unsupported_mismatch.out"

	assertEquals "A does-not-apply answer from a filesystem probe cannot condemn a volume property." \
		"vol=<overlay>" "$(cat "$TEST_TMPDIR/unsupported_mismatch.out")"
}

test_calculate_unsupported_properties_fails_closed_on_source_type_probe_error() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	set +e
	output=$(
		(
			zxfer_run_source_zfs_cmd() {
				printf 'permission denied\n'
				return 3
			}
			zxfer_throw_error() {
				printf '%s|%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			zxfer_calculate_unsupported_properties
		)
	)
	status=$?
	assertEquals 3 "$status"
	assertEquals "Failed to retrieve source dataset types for unsupported-property scan: permission denied|3" "$output"
}

test_calculate_unsupported_properties_fails_closed_on_destination_probe_error() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"
	set +e
	output=$(
		(
			zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
			zxfer_run_source_zfs_cmd() { zxfer_property_test_fake_unsupported_scan source "$@"; }
			zxfer_run_destination_zfs_cmd() { zxfer_property_test_fake_overlay_probe_error "$@"; }
			zxfer_throw_error() {
				printf '%s|%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			PROBE_LOG=/dev/null
			zxfer_calculate_unsupported_properties
		)
	)
	status=$?
	assertEquals 4 "$status"
	assertEquals "Failed to probe destination support for property [overlay] on [backup/dst]: connection reset|4" "$output"
}

test_calculate_unsupported_properties_reports_blank_destination_probe_failures() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"
	set +e
	output=$(
		(
			zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
			zxfer_run_source_zfs_cmd() { zxfer_property_test_fake_unsupported_scan source "$@"; }
			zxfer_run_destination_zfs_cmd() { zxfer_property_test_fake_overlay_probe_blank "$@"; }
			zxfer_test_stub_throw_error_to_stdout status
			PROBE_LOG=/dev/null
			zxfer_calculate_unsupported_properties
		)
	)
	status=$?
	assertEquals 1 "$status"
	assertEquals "Failed to probe destination support for property [overlay] on [backup/dst]: probe exited nonzero without stdout/stderr" "$output"
}

test_calculate_unsupported_properties_fails_closed_when_probe_dataset_lookups_fail() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"
	set +e
	output=$(
		(
			zxfer_probe_destination_existence() {
				g_zxfer_destination_exists_error="Failed to determine whether destination dataset [backup/dst] exists: timeout"
				return 1
			}
			zxfer_run_source_zfs_cmd() { zxfer_property_test_fake_unsupported_scan source "$@"; }
			zxfer_test_stub_throw_error_to_stdout status
			PROBE_LOG=/dev/null
			zxfer_calculate_unsupported_properties
		)
	)
	status=$?
	assertEquals 1 "$status"
	assertEquals "Failed to determine whether destination dataset [backup/dst] exists: timeout" "$output"

	output=$(
		(
			zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
			zxfer_run_source_zfs_cmd() { zxfer_property_test_fake_unsupported_scan source "$@"; }
			zxfer_run_destination_zfs_cmd() {
				printf 'no such dataset\n'
				return 2
			}
			zxfer_throw_error() {
				printf '%s|%s\n' "$1" "${2:-1}"
				exit "${2:-1}"
			}
			PROBE_LOG=/dev/null
			zxfer_calculate_unsupported_properties
		)
	)
	status=$?
	assertEquals 2 "$status"
	assertEquals "Failed to determine the destination property-support probe dataset type for [backup/dst]: no such dataset|2" "$output"
}

test_calculate_unsupported_properties_preserves_caller_ifs_and_globbing() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"
	l_saved_ifs=$IFS
	IFS=","
	set -f
	PROBE_LOG=/dev/null
	(
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
		zxfer_run_source_zfs_cmd() { zxfer_property_test_fake_unsupported_scan source "$@"; }
		zxfer_run_destination_zfs_cmd() { zxfer_property_test_fake_unsupported_scan destination "$@"; }
		zxfer_calculate_unsupported_properties
		printf '%s\n' "$g_zxfer_unsupported_filesystem_properties"
	) >"$TEST_TMPDIR/unsupported_ifs.out"
	l_globbing=$(zxfer_property_test_report_globbing_state after)
	l_ifs_after=$IFS
	set +f
	IFS=$l_saved_ifs
	assertEquals "overlay,volmode" "$(cat "$TEST_TMPDIR/unsupported_ifs.out")"
	assertEquals "after_globbing=disabled" "$l_globbing"
	assertEquals "," "$l_ifs_after"
}

# Fake zfs for a wide -U scan: every source is a filesystem except
# tank/src/c200, a volume. Each type batch logs "batch <count>" and then its
# names to PROBE_LOG; inventories have no differences, so nothing is probed.
zxfer_property_test_fake_wide_scan() {
	case "$2 $3 $4 $5" in
	"get -Hpo name,value type")
		shift 5
		printf 'batch %s\n' "$#" >>"$PROBE_LOG"
		printf '%s\n' "$@" >>"$PROBE_LOG"
		for l_fake_name in "$@"; do
			case $l_fake_name in
			tank/src/c200) printf '%s\tvolume\n' "$l_fake_name" ;;
			*) printf '%s\tfilesystem\n' "$l_fake_name" ;;
			esac
		done
		;;
	"get -Hpo property all")
		printf 'inventory %s %s\n' "$1" "$6" >>"$PROBE_LOG"
		printf 'compression\n'
		;;
	"get -Hpo value type") printf 'filesystem\n' ;;
	*)
		printf 'unexpected zfs command: %s\n' "$*" >&2
		return 1
		;;
	esac
}

test_calculate_unsupported_properties_reads_source_types_in_batches_of_128() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_destination="backup/dst"
	g_recursive_source_list="tank/src"
	l_index=1
	while [ "$l_index" -lt 300 ]; do
		g_recursive_source_list="$g_recursive_source_list
tank/src/c$l_index"
		l_index=$((l_index + 1))
	done
	PROBE_LOG="$TEST_TMPDIR/unsupported_wide.log"
	: >"$PROBE_LOG"
	(
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
		zxfer_run_source_zfs_cmd() { zxfer_property_test_fake_wide_scan source "$@"; }
		zxfer_run_destination_zfs_cmd() { zxfer_property_test_fake_wide_scan destination "$@"; }
		zxfer_calculate_unsupported_properties
	)
	assertEquals "The scan must finish." 0 "$?"

	assertEquals "batch 128
batch 128
batch 44" "$(grep '^batch ' "$PROBE_LOG")"
	assertEquals "Every source is typed once, in order." \
		"$g_recursive_source_list" "$(grep '^tank/' "$PROBE_LOG")"
	assertEquals "Each type is inventoried from its first source, even in a later batch." \
		"inventory source tank/src
inventory destination backup/dst
inventory source tank/src/c200
inventory destination backup/dst/c200" "$(grep '^inventory ' "$PROBE_LOG")"
}

test_calculate_unsupported_properties_scans_every_type_when_remote_calls_read_stdin() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src
tank/src/vol"
	g_destination="backup/dst"
	PROBE_LOG="$TEST_TMPDIR/unsupported_stdin.log"
	: >"$PROBE_LOG"
	(
		# An -O or -T ssh reads stdin; each stand-in drains it the same way.
		g_option_O_origin_host="origin.example"
		zxfer_probe_destination_existence() {
			cat >/dev/null
			g_zxfer_destination_exists_result=1
		}
		zxfer_run_source_zfs_cmd() {
			cat >/dev/null
			zxfer_property_test_fake_unsupported_scan source "$@"
		}
		zxfer_run_destination_zfs_cmd() {
			cat >/dev/null
			zxfer_property_test_fake_unsupported_scan destination "$@"
		}
		zxfer_calculate_unsupported_properties
		printf 'fs=<%s> vol=<%s>\n' "$g_zxfer_unsupported_filesystem_properties" \
			"$g_zxfer_unsupported_volume_properties"
	) >"$TEST_TMPDIR/unsupported_stdin.out"
	assertEquals "Both dataset types are scanned." \
		"fs=<overlay,volmode> vol=<overlay>" "$(cat "$TEST_TMPDIR/unsupported_stdin.out")"
}

test_append_unsupported_property_appends_without_duplicates_per_type() {
	zxfer_append_unsupported_property filesystem overlay
	zxfer_append_unsupported_property filesystem overlay
	zxfer_append_unsupported_property filesystem snapdev
	zxfer_append_unsupported_property volume volmode
	assertEquals "overlay,snapdev" "$g_zxfer_unsupported_filesystem_properties"
	assertEquals "volmode" "$g_zxfer_unsupported_volume_properties"
	zxfer_reset_property_runtime_state
	assertEquals "" "$g_zxfer_unsupported_filesystem_properties"
}

################################################################################
# CREATE-TIME POLICY
################################################################################

test_filter_child_creation_overrides_for_parent_drops_inheritable_overrides_the_parent_supplies() {
	assertEquals "quota=1G=override,compression=lz4=local,atime=on=override" \
		"$(zxfer_filter_child_creation_overrides_for_parent \
			"checksum=sha256=override,quota=1G=override,compression=lz4=local,atime=on=override" \
			"checksum=sha256=local,quota=1G=local,atime=off=local")"
}
