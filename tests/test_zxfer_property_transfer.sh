#!/bin/sh
#
# shunit2 entry point for src/zxfer_property_transfer.sh: its pure helpers
# (the readonly list, the -o reader and check, the child filter and
# inheritance awk) and the branches the black-box suites cannot reach (the
# -U scan edge cases, create-metadata probes, create guards, awk failures).
# The plan, creates, sets, inherits and their failures are pinned black-box in
# tests/test_contract_properties.sh.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/property_fixtures.sh
. "$TESTS_DIR/helpers/property_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_property_transfer"
	zxfer_test_property_fixture_one_time_setup
}

setUp() {
	zxfer_test_property_fixture_setup
}

test_prepare_readonly_property_policy_uses_the_destination_platform_and_migration_mode() {
	zxfer_prepare_readonly_property_policy
	assertEquals "readonly,mountpoint" "$g_zxfer_property_readonly_policy"
	g_destination_operating_system="FreeBSD"
	zxfer_prepare_readonly_property_policy
	assertEquals "FreeBSD appends its list." "readonly,mountpoint,aclmode" \
		"$g_zxfer_property_readonly_policy"
	g_destination_operating_system="SunOS"
	zxfer_prepare_readonly_property_policy
	assertEquals "A newly prepared policy replaces the previous platform's list." \
		"readonly,mountpoint" "$g_zxfer_property_readonly_policy"
	g_option_m_migrate=1
	zxfer_prepare_readonly_property_policy
	g_option_m_migrate=0
	assertEquals "-m leaves mountpoint out, so it moves." "readonly" \
		"$g_zxfer_property_readonly_policy"
}

test_prepared_property_policy_survives_scratch_reads_and_support_scans_until_session_reset() {
	g_option_o_override_property='user:note=a=b\,c;%,compression=lz4'
	g_destination_operating_system="FreeBSD"
	zxfer_consistency_check
	zxfer_prepare_readonly_property_policy
	zxfer_read_override_properties 'compression=gzip'
	g_initial_source=""
	g_recursive_source_list=""
	zxfer_calculate_unsupported_properties
	assertEquals "Scratch parsing must not replace the validated session overrides." \
		'user:note=a%3Db%2Cc%3B%25=override,compression=lz4=override' \
		"$g_zxfer_property_override_policy"
	assertEquals "A support scan must not reset readonly policy." \
		"readonly,mountpoint,aclmode" "$g_zxfer_property_readonly_policy"
	zxfer_reset_session_state
	assertEquals "A new session must discard both old or inherited policies." \
		":" "$g_zxfer_property_readonly_policy:$g_zxfer_property_override_policy"
}

################################################################################
# CREATE METADATA
################################################################################

# The type and zvol size come from the property list, and each is probed
# alone only when the list lacks it; a failed probe fails closed with the zfs
# diagnostic. zfs get all always lists both, so the contract suite pins the
# list values, a bad type and an empty size.
test_get_validated_source_dataset_create_metadata_probes_only_what_the_list_lacks() {
	: >"$TEST_TMPDIR/metadata_probe.log"
	(
		zxfer_run_source_zfs_cmd() {
			printf '%s\n' "$*" >>"$TEST_TMPDIR/metadata_probe.log"
			case "$*" in
			"get -Hpo value type tank/vol") printf 'volume\n' ;;
			"get -Hpo value volsize tank/vol") printf '2147483648\n' ;;
			*)
				printf 'permission denied\n'
				return 5
				;;
			esac
		}
		zxfer_get_validated_source_dataset_create_metadata "tank/vol" "compression=lz4=local"
		printf 'status=%s type=%s volsize=%s\n' "$?" "$g_zxfer_source_dataset_type_result" \
			"$g_zxfer_source_volume_size_result"
		zxfer_get_validated_source_dataset_create_metadata "tank/src" ""
		printf 'status=%s error=%s\n' "$?" "$g_zxfer_property_error_result"
		zxfer_get_validated_source_dataset_create_metadata "tank/other" "type=volume=-"
		printf 'status=%s error=%s\n' "$?" "$g_zxfer_property_error_result"
	) >"$TEST_TMPDIR/metadata_probe.out"
	assertEquals "status=0 type=volume volsize=2147483648
status=5 error=Failed to retrieve source dataset type for [tank/src]: permission denied
status=5 error=Failed to retrieve source zvol size for [tank/other]: permission denied" \
		"$(cat "$TEST_TMPDIR/metadata_probe.out")"
	assertEquals "get -Hpo value type tank/vol
get -Hpo value volsize tank/vol
get -Hpo value type tank/src
get -Hpo value volsize tank/other" "$(cat "$TEST_TMPDIR/metadata_probe.log")"
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

test_calculate_unsupported_properties_fails_closed_when_the_probe_dataset_lookup_fails() {
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

test_filter_child_creation_overrides_for_parent_drops_only_inheritable_overrides_the_parent_supplies() {
	assertEquals "A matching inheritable override goes; a noninheritable one (quota), a non-override and a differing one stay." \
		"quota=1G=override,compression=lz4=local,atime=on=override" \
		"$(zxfer_property_test_filter_child_creation \
			"checksum=sha256=override,quota=1G=override,compression=lz4=local,atime=on=override" \
			"checksum=sha256=local,quota=1G=local,atime=off=local" "")"
	assertEquals "A parent value on the readonly or -I list does not supply an override." \
		"checksum=sha256=override,copies=2=override" \
		"$(
			g_option_I_ignore_properties="copies"
			zxfer_property_test_filter_child_creation \
				"checksum=sha256=override,copies=2=override,atime=off=override" \
				"checksum=sha256=local,copies=2=local,atime=off=local" "checksum"
		)"
}

zxfer_property_test_read_override() {
	(
		zxfer_throw_usage_error() {
			printf 'usage: %s\n' "$1"
			exit 2
		}
		zxfer_read_override_properties "$1"
		printf 'list: <%s>\n' "$g_zxfer_override_properties_result"
	)
	printf 'status: %s\n' "$?"
}

# The -o reader keeps the items in -o order with their names and values
# encoded; "\," is the only escape and empty items are skipped. An item
# without a name and a property named twice are usage errors, the first one
# in list order wins, and the message shows a hostile name escaped.
test_read_override_properties_serializes_escapes_and_refuses_bad_lists() {
	assertEquals "list: <user:note=a%3Db%2Cc%3B%25=override,compression=lz4=override>
status: 0" "$(zxfer_property_test_read_override 'user:note=a=b\,c;%,compression=lz4')"
	assertEquals "An empty -o text is an empty list." "list: <>
status: 0" "$(zxfer_property_test_read_override "")"
	assertEquals "Empty items are skipped." "list: <compression=lz4=override,atime=off=override>
status: 0" "$(zxfer_property_test_read_override ",compression=lz4,,atime=off,")"
	# The last value ends in a backslash that no comma follows.
	l_trailing_backslash_text="user:path=C:\\new,user:end=x\\"
	assertEquals 'list: <user:path=C:\new=override,user:end=x\=override>
status: 0' "$(zxfer_property_test_read_override "$l_trailing_backslash_text")"
	assertEquals "A doubled backslash before a comma keeps one backslash and the comma." \
		'list: <user:a=x\%2Cuser:b%3Dy=override>
status: 0' "$(zxfer_property_test_read_override 'user:a=x\\,user:b=y')"
	assertEquals "Names are compared whole, not as prefixes." "list: <user:a=1=override,user:ab=2=override>
status: 0" "$(zxfer_property_test_read_override "user:a=1,user:ab=2")"
	for l_bad_override in "compression" "=lz4" "==lz4" "compression=lz4,atime" "b,a=1,a=2"; do
		assertEquals "-o $l_bad_override is a syntax error." \
			"usage: Invalid option property - check -o list for syntax errors.
status: 2" "$(zxfer_property_test_read_override "$l_bad_override")"
	done
	for l_repeated in "compression=lz4,compression=gzip" "compression=lz4,atime=off,compression=lz4" \
		"compression=1,compression=2,b"; do
		assertEquals "-o $l_repeated names compression twice." \
			"usage: Duplicate property for -o override: compression.
status: 2" "$(zxfer_property_test_read_override "$l_repeated")"
	done
	assertEquals "Names compare after escaped commas are joined." \
		"usage: Duplicate property for -o override: a,b.
status: 2" "$(zxfer_property_test_read_override 'a\,b=1,a\,b=2')"
	l_hostile_name=$(printf 'user:x\033[2J')
	l_output=$(zxfer_property_test_read_override "$l_hostile_name=1,$l_hostile_name=2")
	assertEquals "The message holds no raw control byte." \
		0 "$(zxfer_property_test_count_control_bytes "$l_output")"
	case $l_output in
	*'usage: Duplicate property for -o override: user:x\x1B[2J.'*) ;;
	*) fail "The name should be shown escaped: $l_output" ;;
	esac
}

################################################################################
# INITIAL-SOURCE -o CHECK
################################################################################

# Purpose: Check one -o text against a source list in a subshell and print
# the status and any usage error.
# Usage: zxfer_property_test_check_override SOURCE_PVS OVERRIDE_TEXT
zxfer_property_test_check_override() {
	(
		zxfer_throw_usage_error() {
			printf 'usage: %s\n' "$1"
			exit 2
		}
		zxfer_read_override_properties "$2"
		zxfer_check_override_properties_on_source "$g_zxfer_override_properties_result" "$1"
	)
	printf 'status: %s\n' "$?"
}

# Each -o name must be a whole property of the initial source's list, read
# literally: an escape in a source value neither hides nor invents one.
test_check_override_properties_on_source_requires_each_name_whole_and_literal() {
	assertEquals "An empty -o list always passes." "status: 0" \
		"$(zxfer_property_test_check_override "compression=lz4=local" "")"
	assertEquals "status: 0" \
		"$(zxfer_property_test_check_override "compression=lz4=local,atime=on=default" "atime=off,compression=gzip")"
	assertEquals "usage: Missing source property for -o override: copies.
status: 2" "$(zxfer_property_test_check_override "compression=lz4=local" "compression=gzip,copies=2")"
	assertEquals "A prefix of a source property is not that property." \
		"usage: Missing source property for -o override: compress.
status: 2" "$(zxfer_property_test_check_override "compression=lz4=local" "compress=gzip")"
	assertEquals "A backslash-zero value earlier in the source list must not hide later properties." \
		"status: 0" "$(zxfer_property_test_check_override \
			'com.x:path=C:\0data=local,compression=lz4=local' "compression=zstd")"
	assertEquals "An octal comma escape in a source value must not invent a property." \
		"usage: Missing source property for -o override: bogusprop.
status: 2" "$(zxfer_property_test_check_override 'com.x:path=x\054bogusprop=local' "bogusprop=1")"
	assertEquals "An encoded name is reported decoded." \
		"usage: Missing source property for -o override: a,b.
status: 2" "$(zxfer_property_test_check_override "compression=lz4=local" 'a\,b=1')"
}

################################################################################
# PLAN
################################################################################

test_property_plan_api_distinguishes_creation_from_an_empty_destination() {
	g_option_P_transfer_property=1
	g_zxfer_property_readonly_policy=""
	g_zxfer_property_override_policy=""
	g_option_I_ignore_properties=""
	for l_api_exists in 0 1; do
		l_api_verb=create
		[ "$l_api_exists" -eq 0 ] || l_api_verb='set'
		zxfer_plan_property_changes 1 "$l_api_exists" filesystem 'compression=lz4=local' "" ""
		assertEquals "Empty payload does not decide whether the dataset exists." \
			"$l_api_verb${ZXFER_TAB}compression=lz4=local=${ZXFER_LF}__ZXFER_PROPERTY_ACTIONS__" \
			"$(cat "$g_zxfer_property_plan_file")"
	done
	# Policies are session inputs; changing one replaces the reused plan.
	g_zxfer_property_readonly_policy=compression
	zxfer_plan_property_changes 1 1 filesystem 'compression=lz4=local' "" ""
	assertEquals '0 0' "$g_zxfer_property_plan_summary_result"
	g_zxfer_property_override_policy='compression=gzip=override'
	g_option_P_transfer_property=0
	g_option_I_ignore_properties=compression
	zxfer_plan_property_changes 1 1 filesystem 'compression=lz4=local' "" ""
	assertEquals "An explicit override survives readonly and ignored policies without -P." \
		"set${ZXFER_TAB}compression=gzip=override=gzip${ZXFER_LF}__ZXFER_PROPERTY_ACTIONS__" \
		"$(cat "$g_zxfer_property_plan_file")"
	g_zxfer_property_override_policy=""
	g_zxfer_property_readonly_policy=""
	g_option_I_ignore_properties=""
	g_option_P_transfer_property=1
	zxfer_plan_property_changes 1 1 filesystem 'compression=lz4=local' "" compression
	assertEquals "The dataset-specific unsupported policy remains explicit." '0 0' "$g_zxfer_property_plan_summary_result"
}

test_property_plan_api_rejects_ambiguous_state_before_staging() {
	# Define the override outside command substitution: ksh otherwise retains
	# the previously defined reporting function inside the substitution.
	zxfer_throw_error() {
		printf '%s|summary=%s\n' "$1" "$g_zxfer_property_plan_summary_result"
		exit 19
	}
	for l_api_bad_state in 2 -1 ''; do
		l_api_failure=$(
			g_zxfer_property_plan_summary_result='stale counts'
			zxfer_plan_property_changes 1 "$l_api_bad_state" filesystem "" "" ""
		)
		assertEquals 19 "$?"
		assertEquals 'Failed to plan dataset properties.|summary=' "$l_api_failure"
	done
	l_api_failure=$(
		g_zxfer_property_plan_summary_result='stale counts'
		zxfer_plan_property_changes 1 1 filesystem "" ""
	)
	assertEquals "An omitted destination list is rejected even if empty was intended." 19 "$?"
	assertEquals 'Failed to plan dataset properties.|summary=' "$l_api_failure"
	assertEquals "Invalid inputs never allocate a staging file." "" "$g_zxfer_property_input_file"
	unset -f zxfer_throw_error
	zxfer_source_module zxfer_reporting.sh
}

# A plan that awk cannot run, or that ends before its marker, fails closed
# with none of its results published (the fault injector cannot fail awk).
test_property_plan_keeps_encoded_actions_and_selects_one_dataset_role() {
	zxfer_property_test_plan 1 'compression=lz4=local,user:note=x%0Ay=local' "" 1 filesystem "" "" "" 'compression=off=local'
	assertEquals 'compression=lz4,user:note=x%0Ay' "$(zxfer_property_plan_list "$g_zxfer_property_plan_file" set)"
	assertEquals '2 0' "$g_zxfer_property_plan_summary_result"
	assertEquals "Encoded values and source provenance stay in one action record." \
		"set${ZXFER_TAB}compression=lz4=local=${ZXFER_LF}set${ZXFER_TAB}user:note=x%0Ay=local=${ZXFER_LF}__ZXFER_PROPERTY_ACTIONS__" \
		"$(cat "$g_zxfer_property_plan_file")"
	zxfer_property_test_plan 0 'compression=lz4=inherited' "" 1 filesystem "" "" "" 'compression=off=local'
	assertEquals "A child gets the selected inheritance action." compression=lz4 \
		"$(zxfer_property_plan_list "$g_zxfer_property_plan_file" inherit)"
	assertEquals "A child does not retain an unused root set." "" "$(zxfer_property_plan_list "$g_zxfer_property_plan_file" set)"
	zxfer_property_test_plan 1 "" "" 0 volume "" "" ""
	assertEquals "A new empty plan replaces the previous actions." __ZXFER_PROPERTY_ACTIONS__ "$(cat "$g_zxfer_property_plan_file")"
	assertEquals '0 0' "$g_zxfer_property_plan_summary_result"
}

test_property_plan_root_and_child_decisions_preserve_value_and_provenance_rules() {
	# Explicit expectations for the initial root and an unresolved child.
	# Every destination exists; "missing" means the property is absent.
	while IFS='|' read -r l_decision_property l_decision_source l_decision_dest_value \
		l_decision_dest_source l_decision_root l_decision_child; do
		l_decision_destination=""
		[ "$l_decision_dest_value" = missing ] ||
			l_decision_destination=$l_decision_property=$l_decision_dest_value=$l_decision_dest_source
		for l_decision_initial in 1 0; do
			l_decision_expected=$l_decision_root
			[ "$l_decision_initial" -eq 1 ] || l_decision_expected=$l_decision_child
			zxfer_property_test_plan "$l_decision_initial" "$l_decision_property=one=$l_decision_source" \
				"" 1 filesystem "" "" "" "$l_decision_destination"
			l_decision_rows=__ZXFER_PROPERTY_ACTIONS__
			l_decision_count=0
			if [ "$l_decision_expected" != none ]; then
				l_decision_rows="$l_decision_expected$ZXFER_TAB$l_decision_property=one=$l_decision_source=$ZXFER_LF$l_decision_rows"
				l_decision_count=1
			fi
			assertEquals "$l_decision_property/$l_decision_source/$l_decision_dest_value/$l_decision_dest_source root=$l_decision_initial" \
				"$l_decision_rows" "$(cat "$g_zxfer_property_plan_file")"
			assertEquals "Only selected actions are counted." "$l_decision_count 0" "$g_zxfer_property_plan_summary_result"
		done
	done <<'EOF'
compression|local|missing|-|set|set
compression|inherited|missing|-|none|inherit
compression|default|missing|-|none|inherit
compression|received|missing|-|none|inherit
compression|local|one|local|none|none
compression|local|one|inherited|set|set
compression|inherited|one|local|none|inherit
compression|inherited|one|inherited|none|none
compression|default|one|local|none|inherit
compression|received|one|inherited|none|none
compression|local|two|local|set|set
compression|local|two|inherited|set|set
compression|inherited|two|local|set|inherit
compression|inherited|two|inherited|set|inherit
compression|default|two|local|set|inherit
quota|inherited|missing|-|none|set
quota|inherited|one|local|none|none
quota|inherited|one|inherited|none|none
quota|inherited|two|local|set|set
quota|local|one|inherited|set|set
EOF
}

test_property_plan_preserves_repeated_record_order_values_and_first_provenance() {
	l_repeated_source='compression=one=override,quota=none=local,compression=two=local,user:note=a%0Ab=local'
	for l_repeated_initial in 1 0; do
		zxfer_property_test_plan "$l_repeated_initial" "$l_repeated_source" "" 1 filesystem "" "" "" \
			'compression=zero=local'
		l_repeated_first_verb='set'
		[ "$l_repeated_initial" -eq 1 ] || l_repeated_first_verb=inherit
		assertEquals "Repeated records keep distinct values and the first override value/provenance." \
			"$l_repeated_first_verb${ZXFER_TAB}compression=one=override=one${ZXFER_LF}set${ZXFER_TAB}quota=none=local=${ZXFER_LF}set${ZXFER_TAB}compression=two=override=one${ZXFER_LF}set${ZXFER_TAB}user:note=a%0Ab=local=${ZXFER_LF}__ZXFER_PROPERTY_ACTIONS__" \
			"$(cat "$g_zxfer_property_plan_file")"
		assertEquals "Repeated override provenance is counted separately from action values." '4 2' "$g_zxfer_property_plan_summary_result"
	done
}

test_property_plan_refuses_late_creation_mismatches_before_emitting_any_action() {
	zxfer_stage_property_inputs "source${ZXFER_TAB}compression=one=local,casesensitivity=sensitive=local" \
		"override${ZXFER_TAB}" "destination${ZXFER_TAB}compression=two=local,casesensitivity=insensitive=local"
	zxfer_get_temp_file
	l_creation_guard_plan=$g_zxfer_runtime_artifact_path_result
	l_creation_guard_status=0
	l_creation_guard_output=$(
		ZXFER_PROPERTY_INPUT_FILE=$g_zxfer_property_input_file ZXFER_PROPERTY_PLAN_FILE=$l_creation_guard_plan \
			ZXFER_AWK_REMOVE_LIST='' ZXFER_AWK_UNSUPPORTED_LIST='' \
			"$g_cmd_awk" -v is_initial=1 -v has_destination=1 -v transfer_all_flag=1 \
			-v source_dstype=filesystem -v required_creation_properties="$ZXFER_REQUIRED_CREATION_PROPERTIES" \
			-v noninheritable_properties="$ZXFER_NONINHERITABLE_PROPERTIES" \
			"$ZXFER_PROPERTY_INPUT_AWK$ZXFER_PROPERTY_AWK_LIB$ZXFER_PROPERTY_RULES_AWK$ZXFER_PROPERTY_PLAN_AWK"
	) || l_creation_guard_status=$?
	assertEquals "The creation-time refusal retains status 3." 3 "$l_creation_guard_status"
	assertEquals "The refused property is the original first output line." casesensitivity "$l_creation_guard_output"
	assertEquals "No earlier set action escapes validation." "" "$(cat "$l_creation_guard_plan")"
}

test_plan_property_changes_publishes_nothing_when_awk_fails_or_stops_short() {
	zxfer_property_test_plan 1 'compression=lz4=local' "" 1 filesystem "" "" "" 'compression=off=local'
	cat >"$TEST_TMPDIR/short-plan-awk" <<'EOF'
#!/bin/sh
printf '__ZXFER_PROPERTY_PLAN__\n'
EOF
	cat >"$TEST_TMPDIR/bad-header-awk" <<'EOF'
#!/bin/sh
printf 'unexpected producer output\n'
EOF
	chmod 755 "$TEST_TMPDIR/short-plan-awk" "$TEST_TMPDIR/bad-header-awk"
	for l_broken_awk in "$TEST_TMPDIR/missing-awk" "$TEST_TMPDIR/short-plan-awk" "$TEST_TMPDIR/bad-header-awk"; do
		(
			g_zxfer_property_plan_summary_result=stale
			g_cmd_awk=$l_broken_awk
			zxfer_throw_error() {
				printf '%s|summary=<%s>\n' "$1" "$g_zxfer_property_plan_summary_result"
				exit 1
			}
			zxfer_property_test_plan 1 'compression=lz4=local' "" 1 filesystem "" "" "" 2>/dev/null
		) >"$TEST_TMPDIR/broken-plan.out"
		l_plan_status=$?
		assertEquals 1 "$l_plan_status"
		assertEquals "Failed to plan dataset properties.|summary=<>" "$(cat "$TEST_TMPDIR/broken-plan.out")"
	done
}

# Matching roughly 640 kB lists exceed exec-size limits; only file paths cross
# the environment. An ordinary no-op retains neither payload nor diagnostic rows.
test_property_planners_accept_large_lists_without_exporting_values() {
	l_large_list=$(awk 'BEGIN {
		for (i = 0; i < 8000; i++) value = value "x"
		for (i = 0; i < 80; i++) printf "%suser:p%d=%s=local", (i ? "," : ""), i, value
	}')
	zxfer_property_test_plan 1 "$l_large_list" "" 1 filesystem "" "" "" "$l_large_list"
	l_large_input_file=$g_zxfer_property_input_file
	assertEquals '0 0' "$g_zxfer_property_plan_summary_result"
	assertEquals __ZXFER_PROPERTY_ACTIONS__ "$(cat "$g_zxfer_property_plan_file")"
	zxfer_property_test_write_actions inherit "compression=lz4" "" "compression=lz4=inherited"
	zxfer_resolve_parent_property_plan "$g_zxfer_property_plan_file" "$l_large_list,compression=lz4=local" "" apply
	assertEquals "Large parent reconciliation reuses the staged input." "$l_large_input_file" "$g_zxfer_property_input_file"
	assertEquals "compression=lz4" "$(zxfer_property_plan_list "$g_zxfer_property_parent_plan_file" inherit)"
}

test_property_input_allocation_and_write_failures_publish_no_plan() {
	(
		zxfer_ensure_runtime_artifact_file() { return 37; }
		zxfer_throw_error() {
			printf '%s|%s|<%s>\n' "$1" "$2" "$g_zxfer_property_plan_summary_result"
			exit "$2"
		}
		g_zxfer_property_plan_summary_result=stale
		zxfer_property_test_plan 1 "" "" 0 volume "" "" ""
	) >"$TEST_TMPDIR/plan-allocation.out"
	l_plan_status=$?
	assertEquals 37 "$l_plan_status"
	assertEquals "Failed to stage property planner input.|37|<>" "$(cat "$TEST_TMPDIR/plan-allocation.out")"
	(
		g_zxfer_property_input_file=$g_zxfer_run_tmp_root
		# Keep this fault at the input writer rather than allocating a new path.
		zxfer_ensure_runtime_artifact_file() { g_zxfer_runtime_artifact_path_result=$1; }
		zxfer_throw_error() {
			printf '%s|%s|<%s>\n' "$1" "$2" "$g_zxfer_property_plan_summary_result"
			exit "$2"
		}
		g_zxfer_property_plan_summary_result=stale
		zxfer_property_test_plan 1 "" "" 0 volume "" "" ""
	) >"$TEST_TMPDIR/plan-write.out"
	l_plan_status=$?
	assertEquals 1 "$l_plan_status"
	assertEquals "Failed to stage property planner input.|1|<>" "$(cat "$TEST_TMPDIR/plan-write.out")"

	l_outside_input=$TEST_TMPDIR/outside-property-input
	printf 'untouched\n' >"$l_outside_input"
	g_zxfer_property_input_file=$l_outside_input
	zxfer_stage_property_inputs "source${ZXFER_TAB}compression=lz4=local"
	l_plan_status=$?
	assertEquals "Outside paths are replaced with a run-root file." 0 "$l_plan_status"
	case $g_zxfer_property_input_file in
	"$g_zxfer_run_tmp_root"/*) ;;
	*) fail "Input file escaped run root: $g_zxfer_property_input_file" ;;
	esac
	assertEquals "Outside file stays untouched." "untouched" "$(cat "$l_outside_input")"
}

test_property_planner_rejects_incomplete_or_malformed_input() {
	zxfer_stage_property_inputs "source${ZXFER_TAB}compression=lz4=local"
	for l_bad_input in "" "source${ZXFER_TAB}compression=lz4=local" \
		"broken${ZXFER_LF}__ZXFER_PROPERTY_INPUT__" \
		"source${ZXFER_TAB}a${ZXFER_LF}source${ZXFER_TAB}b${ZXFER_LF}__ZXFER_PROPERTY_INPUT__" \
		"__ZXFER_PROPERTY_INPUT__${ZXFER_LF}trailing"; do
		(
			zxfer_stage_property_inputs() { printf '%s\n' "$l_bad_input" >"$g_zxfer_property_input_file"; }
			zxfer_throw_error() {
				printf '%s|<%s>\n' "$1" "$g_zxfer_property_plan_summary_result"
				exit 1
			}
			g_zxfer_property_plan_summary_result=stale
			zxfer_property_test_plan 1 "" "" 0 volume "" "" ""
		) >"$TEST_TMPDIR/plan-invalid.out"
		l_plan_status=$?
		assertEquals 1 "$l_plan_status"
		assertEquals "Failed to plan dataset properties.|<>" "$(cat "$TEST_TMPDIR/plan-invalid.out")"
	done
}

test_property_plan_rejects_malformed_truncated_and_unreadable_action_files() {
	zxfer_get_temp_file
	l_bad_plan=$g_zxfer_runtime_artifact_path_result
	for l_bad_rows in "" "set${ZXFER_TAB}compression=lz4=local=lz4" \
		"bogus${ZXFER_TAB}compression=lz4=local=lz4${ZXFER_LF}__ZXFER_PROPERTY_ACTIONS__" \
		"set${ZXFER_TAB}compression=lz4=local=extra=bad${ZXFER_LF}__ZXFER_PROPERTY_ACTIONS__" \
		"set${ZXFER_TAB}compression=lz4${ZXFER_TAB}bad=local=${ZXFER_LF}__ZXFER_PROPERTY_ACTIONS__" \
		"__ZXFER_PROPERTY_ACTIONS__${ZXFER_LF}trailing"; do
		printf '%s\n' "$l_bad_rows" >|"$l_bad_plan"
		zxfer_validate_property_plan "$l_bad_plan"
		l_plan_status=$?
		assertEquals 1 "$l_plan_status"
		assertEquals "Invalid plans clear the result." "" "$g_zxfer_property_plan_summary_result"
	done
	printf '__ZXFER_PROPERTY_ACTIONS__' >|"$l_bad_plan"
	zxfer_validate_property_plan "$l_bad_plan"
	l_plan_status=$?
	assertEquals "The completion record must end with LF." 1 "$l_plan_status"
	zxfer_validate_property_plan "$TEST_TMPDIR/missing-plan"
	l_plan_status=$?
	assertEquals 1 "$l_plan_status"
}

test_property_scratch_is_run_owned_reused_and_forgotten_on_reset() {
	l_unowned_input=$TEST_TMPDIR/inherited-property-input
	printf 'unowned\n' >"$l_unowned_input"
	g_zxfer_property_input_file=$l_unowned_input
	g_zxfer_property_plan_file=$l_unowned_input
	g_zxfer_property_parent_plan_file=$l_unowned_input
	zxfer_reset_property_runtime_state
	assertEquals '||' "$g_zxfer_property_input_file|$g_zxfer_property_plan_file|$g_zxfer_property_parent_plan_file"
	zxfer_property_test_plan 0 'compression=lz4=inherited' "" 1 filesystem "" "" "" 'compression=off=local'
	l_owned_input=$g_zxfer_property_input_file
	l_owned_plan=$g_zxfer_property_plan_file
	zxfer_resolve_parent_property_plan "$l_owned_plan" 'compression=lz4=local' "" apply
	l_owned_parent=$g_zxfer_property_parent_plan_file
	zxfer_property_test_plan 0 'compression=lz4=inherited' "" 1 filesystem "" "" "" 'compression=off=local'
	zxfer_resolve_parent_property_plan "$g_zxfer_property_plan_file" 'compression=off=local' "" apply
	assertEquals "$l_owned_input|$l_owned_plan|$l_owned_parent" "$g_zxfer_property_input_file|$g_zxfer_property_plan_file|$g_zxfer_property_parent_plan_file"
	assertEquals 'compression=lz4' "$(zxfer_property_plan_list "$l_owned_parent" set)"
	for l_owned_file in "$l_owned_input" "$l_owned_plan" "$l_owned_parent"; do
		case $l_owned_file in "$g_zxfer_run_tmp_root"/*) ;; *) fail "Outside run root: $l_owned_file" ;; esac
		assertEquals 600 "$(zxfer_get_path_mode_octal "$l_owned_file")"
	done
	assertEquals 'unowned' "$(cat "$l_unowned_input")"
}

test_property_filter_policy_control_characters_do_not_corrupt_input() {
	zxfer_property_test_plan 1 "compression=lz4=local" "" 1 filesystem "" \
		"compression${ZXFER_LF}user:other${ZXFER_TAB}value" "" "compression=off=local"
	zxfer_validate_property_plan "$g_zxfer_property_plan_file"
	l_plan_status=$?
	assertEquals "Policy bytes never acquire action-record syntax." 0 "$l_plan_status"
}

# Input staging must finish first; later allocation/write failures retain their
# statuses and never publish stale action counts.
test_property_plan_output_allocation_and_write_failures_clear_publication() {
	zxfer_stage_property_inputs "source${ZXFER_TAB}"
	(
		zxfer_ensure_runtime_artifact_file() {
			[ "$2" != property-plan ] || return 43
			g_zxfer_runtime_artifact_path_result=$1
		}
		zxfer_throw_error() {
			printf '%s|%s|<%s>\n' "$1" "$2" "$g_zxfer_property_plan_summary_result"
			exit "$2"
		}
		g_zxfer_property_plan_summary_result=stale
		zxfer_property_test_plan 1 "" "" 0 volume "" "" ""
	) >"$TEST_TMPDIR/output-allocation.out"
	l_plan_status=$?
	assertEquals 43 "$l_plan_status"
	assertEquals 'Failed to plan dataset properties.|43|<>' "$(cat "$TEST_TMPDIR/output-allocation.out")"
	(
		g_zxfer_property_plan_file=$g_zxfer_run_tmp_root
		# Keep this fault at the output writer rather than allocating a new path.
		zxfer_ensure_runtime_artifact_file() { g_zxfer_runtime_artifact_path_result=$1; }
		zxfer_throw_error() {
			printf '%s|%s|<%s>\n' "$1" "$2" "$g_zxfer_property_plan_summary_result"
			exit "$2"
		}
		g_zxfer_property_plan_summary_result=stale
		zxfer_property_test_plan 1 "" "" 0 volume "" "" ""
	) >"$TEST_TMPDIR/output-write.out"
	l_plan_status=$?
	assertEquals 1 "$l_plan_status"
	assertEquals 'Failed to plan dataset properties.|1|<>' "$(cat "$TEST_TMPDIR/output-write.out")"
}

test_property_plan_counts_provenance_separately_from_values() {
	zxfer_property_test_plan 0 'user:p=override=local' "" 1 filesystem "" "" ""
	assertEquals "A value named override does not request a parent read." '1 0' "$g_zxfer_property_plan_summary_result"
}

test_property_operation_owns_raw_and_effective_views_across_result_changes() {
	(
		g_option_e_restore_property_mode=1
		g_option_P_transfer_property=1
		g_zxfer_property_override_policy="user:note=override=override"
		zxfer_load_normalized_dataset_properties() {
			case $2 in
			source) g_zxfer_normalized_dataset_properties="type=volume=-,volsize=16384=-,user:note=original=local" ;;
			destination)
				g_zxfer_normalized_dataset_properties="type=volume=-,volsize=16384=-,user:note=old=local"
				g_zxfer_property_row_payload_result=clobbered
				;;
			esac
		}
		zxfer_find_restored_backup_properties() {
			g_zxfer_property_row_payload_result="type=volume=-,volsize=16384=-,user:note=restored=local"
		}
		zxfer_property_destination_exists() { return 0; }
		zxfer_execute_property_plan() {
			printf 'apply=%s\n' "$(zxfer_property_plan_list "$2" set)"
			g_zxfer_normalized_dataset_properties=clobbered
			g_zxfer_required_properties_result=clobbered
			g_zxfer_property_plan_file=clobbered
			g_zxfer_property_plan_summary_result=clobbered
			g_zxfer_property_row_payload_result=clobbered
		}
		zxfer_capture_backup_metadata_for_completed_transfer() { printf 'backup=%s\n' "$2"; }
		zxfer_transfer_properties tank/src
		g_zxfer_property_override_policy=""
		zxfer_transfer_properties tank/src
	) >"$TEST_TMPDIR/owned-property-views.out"
	assertEquals "Overrides and restore only affect the applied view; each backup retains the source." \
		"apply=user:note=override
backup=type=volume=-,volsize=16384=-,user:note=original=local
apply=user:note=restored
backup=type=volume=-,volsize=16384=-,user:note=original=local" \
		"$(cat "$TEST_TMPDIR/owned-property-views.out")"
}

################################################################################
# DESTINATION CREATE
################################################################################

# The launcher notes a seeded dataset again after its seed, so only this test
# sees that a destination found live is added to the destination list.
test_property_destination_exists_live_probes_unlisted_destinations_and_notes_existing_ones() {
	set +e
	(
		g_recursive_dest_list="backup/dst"
		zxfer_probe_destination_existence() {
			printf '%s %s\n' "$1" "${2:-cache}" >>"$TEST_TMPDIR/exists_probe.log"
			g_zxfer_destination_exists_result=1
		}
		zxfer_property_destination_exists "backup/dst/child"
		printf 'status=%s\n' "$?" >>"$TEST_TMPDIR/exists_probe.log"
		printf 'list=%s\n' "$(printf '%s' "$g_recursive_dest_list" | tr '\n' ' ')" >>"$TEST_TMPDIR/exists_probe.log"
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=0; }
		zxfer_property_destination_exists "backup/dst/other"
		printf 'missing_status=%s\n' "$?" >>"$TEST_TMPDIR/exists_probe.log"
		printf 'list=%s\n' "$(printf '%s' "$g_recursive_dest_list" | tr '\n' ' ')" >>"$TEST_TMPDIR/exists_probe.log"
	)
	assertEquals "backup/dst/child live
status=0
list=backup/dst backup/dst/child
missing_status=1
list=backup/dst backup/dst/child" "$(cat "$TEST_TMPDIR/exists_probe.log")"
}

zxfer_property_test_log_destination_zfs() {
	printf '%s\n' "$*" >>"$CREATE_LOG"
}

# Existence fakes, defined at top level because a case statement inside
# "$(...)" is not portable across shells: only the parent backup/dst exists;
# or the parent probe fails operationally.
zxfer_property_test_parent_exists() {
	case "$1" in
	backup/dst) g_zxfer_destination_exists_result=1 ;;
	*) g_zxfer_destination_exists_result=0 ;;
	esac
}

zxfer_property_test_parent_probe_fails() {
	g_zxfer_destination_exists_error="Failed to determine whether destination dataset [backup/dst] exists: permission denied"
	return 1
}

test_create_destination_dataset_uses_parent_create_when_no_properties_apply() {
	CREATE_LOG="$TEST_TMPDIR/create_parents.log"
	: >"$CREATE_LOG"
	(
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=0; }
		zxfer_run_destination_zfs_cmd() { zxfer_property_test_log_destination_zfs "$@"; }
		zxfer_property_test_create 1 "" "" filesystem "" "backup/dst/child" "readonly"
	)
	assertEquals "create -p backup/dst/child" "$(cat "$CREATE_LOG")"
}

test_create_destination_dataset_stops_before_any_create_when_the_child_override_filter_fails() {
	l_failing_awk="$TEST_TMPDIR/failing-awk"
	cat >"$l_failing_awk" <<'EOF'
#!/bin/sh
exit 6
EOF
	chmod 755 "$l_failing_awk"
	set +e
	output=$(
		(
			g_cmd_awk=$l_failing_awk
			zxfer_probe_destination_existence() { zxfer_property_test_parent_exists "$@"; }
			zxfer_load_normalized_dataset_properties() {
				g_zxfer_normalized_dataset_properties="compression=lz4=local"
			}
			zxfer_run_destination_zfs_cmd() { printf 'unexpected zfs %s\n' "$*"; }
			zxfer_throw_error() {
				printf '%s|%s\n' "$1" "$2"
				exit "$2"
			}
			zxfer_property_test_create 0 "" "compression=lz4=override" filesystem "" \
				"backup/dst/child" "readonly"
		)
	)
	status=$?
	assertEquals 6 "$status"
	assertEquals "A failed child override filter must stop the run before any create." \
		"Failed to filter child creation override properties.|6" "$output"
}

test_create_destination_dataset_reports_parent_probe_failures() {
	set +e
	output=$(
		(
			zxfer_probe_destination_existence() { zxfer_property_test_parent_probe_fails "$@"; }
			zxfer_test_stub_throw_error_to_stdout
			zxfer_property_test_create 1 "compression=lz4=local" "" filesystem "" "backup/dst/child" "readonly"
		)
	)
	status=$?
	assertEquals 1 "$status"
	assertEquals "Failed to determine whether destination dataset [backup/dst] exists: permission denied" "$output"
}

test_run_zfs_create_with_properties_rejects_unsafe_shapes() {
	set +e
	output=$(zxfer_property_test_run_create yes filesystem "" "compression=lz4" "backup/dst" 2>&1)
	status=$?
	assertEquals "Parent hierarchy creates must never carry -o properties." 1 "$status"
	assertEquals "" "$output"
	zxfer_property_test_run_create no volume "" "compression=lz4" "backup/dst" >/dev/null 2>&1
	assertEquals "Volume creates need the source volsize." 1 "$?"
}

################################################################################
# CHILD INHERIT ADJUSTMENT
################################################################################

zxfer_property_test_adjust_with_parent() {
	l_parent_pvs=$1
	shift
	(
		zxfer_property_test_write_actions set "$2" "$3" "$1"
		zxfer_resolve_parent_property_plan "$g_zxfer_property_plan_file" "$l_parent_pvs" "$ZXFER_BASE_READONLY_PROPERTIES" apply
		zxfer_property_plan_list "$g_zxfer_property_parent_plan_file" set
		zxfer_property_plan_list "$g_zxfer_property_parent_plan_file" inherit
	)
}

# The child inherit adjustment against the destination parent (its list
# filtered with the readonly and -I lists first): an inherit stays only where
# the parent provides the value or it is a matching inheritable -o value, and
# is set on the child otherwise; a noninheritable -o value is always set. The
# plan's child sets are local or noninheritable, so its set-list branches are
# pinned here only.
test_adjust_child_inherit_to_match_parent_keeps_inherits_only_the_parent_provides() {
	assertEquals "A mismatched parent value turns an inherit into a set." "quota=32M,atime=off
checksum=sha256" "$(zxfer_property_test_adjust_with_parent "checksum=sha256=local,atime=on=local" \
		"checksum=sha256=inherited,atime=off=inherited" "quota=32M" "checksum=sha256,atime=off")"
	assertEquals "A matching parent keeps the inherits." "
checksum=sha256,atime=off" "$(zxfer_property_test_adjust_with_parent "checksum=sha256=local,atime=off=local" \
		"checksum=sha256=inherited,atime=off=inherited" "" "checksum=sha256,atime=off")"
	assertEquals "A set of an inherited source value becomes an inherit when the parent matches." \
		"compression=lz4
checksum=sha256" "$(zxfer_property_test_adjust_with_parent "checksum=sha256=local,compression=lz4=local" \
			"checksum=sha256=inherited,compression=lz4=local" "checksum=sha256,compression=lz4" "")"
	assertEquals "A matching inheritable override stays inherited; a noninheritable one is set." \
		"quota=1G
checksum=sha256,atime=off" "$(zxfer_property_test_adjust_with_parent "checksum=sha256=local" \
			"checksum=sha256=override,atime=off=override,quota=1G=override" \
			"checksum=sha256,quota=1G" "atime=off")"
	assertEquals "Read-only and -I parent entries are not visible, so the child must set them locally." \
		"readonly=on,copies=2
atime=off" "$(
			g_option_I_ignore_properties="copies"
			zxfer_property_test_adjust_with_parent "readonly=on=local,copies=2=local,atime=off=local" \
				"readonly=on=inherited,copies=2=inherited,atime=off=inherited" "" \
				"readonly=on,copies=2,atime=off"
		)"
}

# The shared resolver preserves creation's underlying status, reconciliation
# diagnostics, and checked publication when a producer stops short.
test_parent_resolution_preserves_failure_statuses_and_rejects_partial_output() {
	zxfer_property_test_write_actions set "compression=lz4" "" "compression=lz4=inherited"
	l_parent_input=$g_zxfer_property_plan_file
	(
		g_cmd_awk="$TEST_TMPDIR/missing-parent-awk"
		zxfer_resolve_parent_property_plan "$l_parent_input" "compression=lz4=local" "" create 2>/dev/null
	) >"$TEST_TMPDIR/parent-failure.out"
	l_parent_status=$?
	assertEquals "Creation preserves the underlying command status." 127 "$l_parent_status"
	(
		g_cmd_awk="$TEST_TMPDIR/missing-parent-awk"
		zxfer_test_stub_throw_error_to_stdout
		zxfer_resolve_parent_property_plan "$l_parent_input" "compression=lz4=local" "" apply 2>/dev/null
	) >"$TEST_TMPDIR/parent-failure.out"
	l_parent_status=$?
	assertEquals "Reconciliation retains its original diagnostic and status." 1 "$l_parent_status"
	assertEquals "Failed to reconcile child property inheritance." "$(cat "$TEST_TMPDIR/parent-failure.out")"
	cat >"$TEST_TMPDIR/short-parent-awk" <<'EOF'
#!/bin/sh
printf 'set\tcompression=lz4=inherited=lz4\n' >"$ZXFER_PROPERTY_PLAN_OUTPUT"
EOF
	chmod 755 "$TEST_TMPDIR/short-parent-awk"
	(
		g_cmd_awk="$TEST_TMPDIR/short-parent-awk"
		zxfer_resolve_parent_property_plan "$l_parent_input" "compression=lz4=local" "" create
		l_parent_status=$?
		printf '%s|<%s>\n' "$l_parent_status" "$g_zxfer_property_plan_summary_result"
	) >"$TEST_TMPDIR/parent-short.out"
	assertEquals "A partial plan never publishes a count summary." '1|<>' "$(cat "$TEST_TMPDIR/parent-short.out")"
}

zxfer_property_test_apply() {
	(
		# Join argv with explicit spaces: "$*" would use the caller's IFS.
		zxfer_run_destination_zfs_cmd() {
			l_apply_line=""
			for l_apply_arg in "$@"; do
				l_apply_line="$l_apply_line $l_apply_arg"
			done
			printf '%s\n' "${l_apply_line# }"
		}
		zxfer_property_test_write_actions set "$4" "$5"
		zxfer_execute_property_plan apply "$g_zxfer_property_plan_file" "" "" "$1"
	)
}

test_property_transfer_helpers_preserve_caller_ifs_and_globbing() {
	g_initial_source="tank/src"
	g_initial_source_had_trailing_slash=1
	g_recursive_source_list="tank/src"
	g_destination="backup/dst"
	PROBE_LOG=/dev/null
	l_saved_ifs=$IFS
	IFS=","
	set -f
	zxfer_read_override_properties "compression=lz4,user:glob=*"
	l_override=$g_zxfer_override_properties_result
	result=$(zxfer_property_test_apply "backup/dst/child" 0 "" "compression=lz4,atime=off" "checksum=sha256")
	zxfer_property_test_plan 1 "compression=lz4=local,atime=off=local" "" 1 filesystem "" "" "" \
		"compression=off=local"
	l_initial_set=$(zxfer_property_plan_list "$g_zxfer_property_plan_file" set)
	l_unsupported=$(
		zxfer_probe_destination_existence() { g_zxfer_destination_exists_result=1; }
		zxfer_run_source_zfs_cmd() { zxfer_property_test_fake_unsupported_scan source "$@"; }
		zxfer_run_destination_zfs_cmd() { zxfer_property_test_fake_unsupported_scan destination "$@"; }
		zxfer_calculate_unsupported_properties
		printf '%s\n' "$g_zxfer_unsupported_filesystem_properties"
	)
	l_globbing=$(zxfer_property_test_report_globbing_state after)
	l_ifs_after=$IFS
	set +f
	IFS=$l_saved_ifs
	assertEquals "compression=lz4=override,user:glob=*=override" "$l_override"
	assertEquals "set compression=lz4 atime=off backup/dst/child
inherit checksum backup/dst/child" "$result"
	assertEquals "compression=lz4,atime=off" "$l_initial_set"
	assertEquals "overlay,volmode" "$l_unsupported"
	assertEquals "after_globbing=disabled" "$l_globbing"
	assertEquals "," "$l_ifs_after"
}

test_empty_property_plan_is_silent_on_apply_and_still_creates_a_dataset() {
	zxfer_property_test_write_actions set ''
	output=$(
		g_option_v_verbose=1
		g_option_n_dryrun=0
		zxfer_run_destination_zfs_cmd() { printf '%s\n' "$*"; }
		zxfer_execute_property_plan apply "$g_zxfer_property_plan_file" "" "" backup/dst &&
			zxfer_execute_property_plan create "$g_zxfer_property_plan_file" filesystem "" backup/dst
	)
	assertEquals 0 "$?"
	assertEquals "Only creation needs a ZFS command for an empty plan." "create backup/dst" "$output"
}

# No mutation may precede detection of a short or malformed action file.
test_property_executor_refuses_partial_or_wrong_mode_plans_before_mutation() {
	zxfer_property_test_write_actions set 'compression=lz4'
	l_execute_test_plan=$g_zxfer_property_plan_file
	(
		zxfer_run_destination_zfs_cmd() { printf 'unexpected command\n'; }
		zxfer_execute_property_plan create "$l_execute_test_plan" filesystem "" backup/dst
		l_execute_test_status=$?
		printf 'wrong-mode=%s\n' "$l_execute_test_status"
		printf 'set\tcompression=lz4=local=\n' >|"$l_execute_test_plan"
		zxfer_execute_property_plan apply "$l_execute_test_plan" "" "" backup/dst
		l_execute_test_status=$?
		printf 'partial=%s\n' "$l_execute_test_status"
	) >"$TEST_TMPDIR/executor-invalid.out"
	assertEquals 'wrong-mode=1
partial=1' "$(cat "$TEST_TMPDIR/executor-invalid.out")"
}

# These cases exercise the operation owner after the old parent coordinator
# was removed: no unnecessary parent reads, and original probe/read statuses.
test_property_owner_preserves_parent_guards_and_failure_ordering() {
	for l_owner_test_mode in empty pool_root missing probe_failure read_failure; do
		(
			g_option_P_transfer_property=1
			g_actual_dest=backup/dst/child
			[ "$l_owner_test_mode" != pool_root ] || g_actual_dest=backup
			zxfer_property_destination_exists() { return 0; }
			zxfer_load_normalized_dataset_properties() {
				g_zxfer_normalized_dataset_properties_cache_hit=1
				if [ "$2" = source ]; then
					g_zxfer_normalized_dataset_properties='type=volume=-,volsize=8192=-,compression=lz4=inherited'
				elif [ "$1" = "$g_actual_dest" ]; then
					g_zxfer_normalized_dataset_properties='type=volume=-,volsize=8192=-,compression=off=local'
					[ "$l_owner_test_mode" != empty ] || g_zxfer_normalized_dataset_properties='type=volume=-,volsize=8192=-,compression=lz4=inherited'
				else
					printf 'parent-read\n'
					return 6
				fi
			}
			zxfer_probe_destination_existence() {
				printf 'parent-probe\n'
				g_zxfer_destination_exists_error='parent probe failed'
				[ "$l_owner_test_mode" != probe_failure ] || return 7
				g_zxfer_destination_exists_result=0
				[ "$l_owner_test_mode" != read_failure ] || g_zxfer_destination_exists_result=1
			}
			zxfer_run_destination_zfs_cmd() { printf '%s\n' "$*"; }
			zxfer_capture_backup_metadata_for_completed_transfer() { :; }
			zxfer_throw_error() {
				printf '%s|%s\n' "$1" "$2"
				exit "$2"
			}
			zxfer_transfer_properties tank/src/child
		) >"$TEST_TMPDIR/owner-$l_owner_test_mode.out"
		l_owner_test_status=$?
		case $l_owner_test_mode in
		empty)
			assertEquals 0 "$l_owner_test_status"
			assertEquals "No actions require no parent probe." '' "$(cat "$TEST_TMPDIR/owner-empty.out")"
			;;
		pool_root)
			assertEquals 0 "$l_owner_test_status"
			assertEquals 'inherit compression backup' "$(cat "$TEST_TMPDIR/owner-pool_root.out")"
			;;
		missing)
			assertEquals 0 "$l_owner_test_status"
			assertEquals 'parent-probe
inherit compression backup/dst/child' "$(cat "$TEST_TMPDIR/owner-missing.out")"
			;;
		probe_failure)
			assertEquals 7 "$l_owner_test_status"
			assertEquals 'parent-probe
parent probe failed|7' "$(cat "$TEST_TMPDIR/owner-probe_failure.out")"
			;;
		read_failure)
			assertEquals 6 "$l_owner_test_status"
			assertEquals 'parent-probe
parent-read
Failed to reconcile inherited child properties for destination [backup/dst/child].|6' "$(cat "$TEST_TMPDIR/owner-read_failure.out")"
			;;
		esac
	done
}

# A staged-file read failure belongs to the operation: never continue into
# capture or the next dataset when the consumer cannot read its retained file.
test_property_owner_stops_before_capture_when_the_action_file_disappears() {
	(
		g_option_P_transfer_property=1
		zxfer_property_destination_exists() { return 0; }
		zxfer_load_normalized_dataset_properties() {
			g_zxfer_normalized_dataset_properties='type=volume=-,volsize=8192=-,compression=lz4=local'
		}
		zxfer_show_property_plan() { rm -f "$1"; }
		zxfer_run_destination_zfs_cmd() { printf 'unexpected mutation\n'; }
		zxfer_capture_backup_metadata_for_completed_transfer() { printf 'unexpected capture\n'; }
		zxfer_throw_error() {
			printf '%s|%s\n' "$1" "$2"
			exit "$2"
		}
		zxfer_transfer_properties tank/src
	) >"$TEST_TMPDIR/missing-action-file.out"
	l_owner_test_status=$?
	assertEquals 1 "$l_owner_test_status"
	assertEquals 'Failed to read dataset property plan.|1' "$(cat "$TEST_TMPDIR/missing-action-file.out")"
}

test_property_owner_skips_plan_execution_on_zero_actions_when_plan_file_is_readable() {
	(
		g_option_P_transfer_property=1
		zxfer_property_destination_exists() { return 0; }
		zxfer_load_normalized_dataset_properties() {
			g_zxfer_normalized_dataset_properties='type=volume=-,volsize=8192=-,compression=lz4=local'
		}
		zxfer_execute_property_plan() { printf 'unexpected execute\n'; }
		zxfer_capture_backup_metadata_for_completed_transfer() { printf 'captured=%s\n' "$1"; }
		zxfer_transfer_properties tank/src
	) >"$TEST_TMPDIR/skip-zero-action-exec.out"
	l_owner_test_status=$?
	assertEquals 0 "$l_owner_test_status"
	assertEquals 'captured=tank/src' "$(cat "$TEST_TMPDIR/skip-zero-action-exec.out")"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
