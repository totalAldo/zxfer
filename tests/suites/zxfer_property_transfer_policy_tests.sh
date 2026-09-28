#!/bin/sh
# Property transfer fragment: the readonly list, source create metadata, the
# -U unsupported-property scan, and the child create-time override filter.
# Run by tests/test_zxfer_property_transfer.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

################################################################################
# READONLY LIST
################################################################################

test_resolve_readonly_properties_appends_freebsd_list() {
	g_destination_operating_system="FreeBSD"
	zxfer_resolve_readonly_properties
	assertEquals "readonly,mountpoint,aclmode" "$g_zxfer_readonly_properties_result"
}

test_resolve_readonly_properties_follows_the_current_platform_on_every_call() {
	g_destination_operating_system="FreeBSD"
	zxfer_resolve_readonly_properties
	g_destination_operating_system="SunOS"
	zxfer_resolve_readonly_properties
	assertEquals "The list is resolved per call, not memoized." \
		"readonly,mountpoint" "$g_zxfer_readonly_properties_result"
}

test_resolve_readonly_properties_removes_mountpoint_during_migration() {
	g_option_m_migrate=1
	zxfer_resolve_readonly_properties
	g_option_m_migrate=0
	assertEquals "readonly" "$g_zxfer_readonly_properties_result"
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
			"checksum=sha256=local,quota=1G=local,atime=off=local" "")"
}

test_filter_child_creation_overrides_for_parent_ignores_readonly_and_ignored_parent_entries() {
	assertEquals "A parent value on the readonly or -I list does not supply an override." \
		"checksum=sha256=override,copies=2=override" \
		"$(
			g_option_I_ignore_properties="copies"
			zxfer_filter_child_creation_overrides_for_parent \
				"checksum=sha256=override,copies=2=override,atime=off=override" \
				"checksum=sha256=local,copies=2=local,atime=off=local" "checksum"
		)"
}

test_filter_child_creation_overrides_for_parent_reports_awk_failures() {
	(
		g_cmd_awk="$TEST_TMPDIR/missing-awk"
		zxfer_filter_child_creation_overrides_for_parent "atime=off=override" \
			"atime=off=local" "" 2>/dev/null
	)
	assertNotEquals "A missing awk fails the filter." 0 "$?"
}
