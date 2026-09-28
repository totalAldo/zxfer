#!/bin/sh
# Property transfer fragment: the -o reader (syntax, the repeated-property
# rule, encoding), the initial-source -o check, and the one-awk plan: derive
# of the override and creation lists, and the diff against an existing
# destination. Run by tests/test_zxfer_property_transfer.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

################################################################################
# -o READER
################################################################################

# Purpose: Read one -o text in a subshell and print the status, then the
# usage error or the serialized override list.
# Usage: zxfer_property_test_read_override TEXT
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

test_read_override_properties_serializes_items_in_order_with_encoded_values() {
	assertEquals "list: <user:note=a%3Db%2Cc%3B%25=override,compression=lz4=override>
status: 0" "$(zxfer_property_test_read_override 'user:note=a=b\,c;%,compression=lz4')"
	assertEquals "An empty -o text is an empty list." "list: <>
status: 0" "$(zxfer_property_test_read_override "")"
	assertEquals "Empty items are skipped." "list: <compression=lz4=override,atime=off=override>
status: 0" "$(zxfer_property_test_read_override ",compression=lz4,,atime=off,")"
}

test_read_override_properties_escapes_only_a_backslash_before_a_comma() {
	# The last value ends in a backslash that no comma follows.
	l_trailing_backslash_text="user:path=C:\\new,user:end=x\\"
	assertEquals 'list: <user:path=C:\new=override,user:end=x\=override>
status: 0' "$(zxfer_property_test_read_override "$l_trailing_backslash_text")"
	assertEquals "A doubled backslash before a comma keeps one backslash and the comma." \
		'list: <user:a=x\%2Cuser:b%3Dy=override>
status: 0' "$(zxfer_property_test_read_override 'user:a=x\\,user:b=y')"
}

test_read_override_properties_rejects_items_without_a_property_name() {
	for l_bad_override in "compression" "=lz4" "==lz4" "compression=lz4,atime"; do
		assertEquals "-o $l_bad_override is a syntax error." \
			"usage: Invalid option property - check -o list for syntax errors.
status: 2" "$(zxfer_property_test_read_override "$l_bad_override")"
	done
}

test_read_override_properties_rejects_a_property_named_twice() {
	assertEquals "usage: Duplicate property for -o override: compression.
status: 2" "$(zxfer_property_test_read_override "compression=lz4,compression=gzip")"
	assertEquals "The same value twice is still refused, as zfs refuses it." \
		"usage: Duplicate property for -o override: compression.
status: 2" "$(zxfer_property_test_read_override "compression=lz4,atime=off,compression=lz4")"
	assertEquals "Names compare after escaped commas are joined." \
		"usage: Duplicate property for -o override: a,b.
status: 2" "$(zxfer_property_test_read_override 'a\,b=1,a\,b=2')"
	assertEquals "Names are compared whole, not as prefixes." "list: <user:a=1=override,user:ab=2=override>
status: 0" "$(zxfer_property_test_read_override "user:a=1,user:ab=2")"
}

test_read_override_properties_reports_the_first_problem_in_list_order() {
	assertEquals "usage: Duplicate property for -o override: a.
status: 2" "$(zxfer_property_test_read_override "a=1,a=2,b")"
	assertEquals "usage: Invalid option property - check -o list for syntax errors.
status: 2" "$(zxfer_property_test_read_override "b,a=1,a=2")"
}

test_read_override_properties_escapes_control_bytes_in_the_duplicate_message() {
	l_hostile_name=$(printf 'user:x\033[2J')
	l_output=$(zxfer_property_test_read_override "$l_hostile_name=1,$l_hostile_name=2")
	assertEquals "The message holds no raw control byte." \
		0 "$(zxfer_property_test_count_control_bytes "$l_output")"
	case $l_output in
	*'usage: Duplicate property for -o override: user:x\x1B[2J.'*) ;;
	*) fail "The name should be shown escaped: $l_output" ;;
	esac
}

test_read_override_properties_preserves_caller_ifs_and_globbing() {
	l_saved_ifs=$IFS
	IFS=","
	set -f
	zxfer_read_override_properties "compression=lz4,user:glob=*"
	l_globbing=$(zxfer_property_test_report_globbing_state after)
	l_ifs_after=$IFS
	set +f
	IFS=$l_saved_ifs
	assertEquals "compression=lz4=override,user:glob=*=override" "$g_zxfer_override_properties_result"
	assertEquals "after_globbing=disabled" "$l_globbing"
	assertEquals "," "$l_ifs_after"
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

test_check_override_properties_on_source_requires_every_override_on_the_source() {
	assertEquals "An empty -o list always passes." "status: 0" \
		"$(zxfer_property_test_check_override "compression=lz4=local" "")"
	assertEquals "status: 0" \
		"$(zxfer_property_test_check_override "compression=lz4=local,atime=on=default" "atime=off,compression=gzip")"
	assertEquals "usage: Missing source property for -o override: copies.
status: 2" "$(zxfer_property_test_check_override "compression=lz4=local" "compression=gzip,copies=2")"
	assertEquals "A prefix of a source property is not that property." \
		"usage: Missing source property for -o override: compress.
status: 2" "$(zxfer_property_test_check_override "compression=lz4=local" "compress=gzip")"
}

test_check_override_properties_on_source_reads_source_values_literally() {
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
# PLAN: DERIVE
################################################################################

# Purpose: Read an -o text and plan without a destination, then print the
# override and creation lists.
# Usage: zxfer_property_test_derive SOURCE_PVS OVERRIDE_TEXT TRANSFER_ALL
# DATASET_TYPE [READONLY_CSV] [IGNORE_CSV] [UNSUPPORTED_CSV]
zxfer_property_test_derive() {
	zxfer_read_override_properties "$2"
	zxfer_plan_property_changes "$1" "$g_zxfer_override_properties_result" "$3" "$4" \
		"${5:-}" "${6:-}" "${7:-}"
	printf '%s\n%s\n' "$g_zxfer_plan_override_pvs_result" "$g_zxfer_plan_creation_pvs_result"
}

test_plan_property_changes_preserves_override_only_mode_order() {
	assertEquals "compression=lz4=override,quota=1G=override
compression=lz4=override,quota=1G=override" \
		"$(zxfer_property_test_derive "" "compression=lz4,quota=1G" 0 filesystem)"
}

test_plan_property_changes_applies_overrides_the_source_lacks_without_p() {
	assertEquals "The plan never checks -o names; a child without -P still applies them." \
		"copies=2=override
copies=2=override" "$(zxfer_property_test_derive "compression=lz4=local" "copies=2" 0 filesystem)"
	assertEquals "With -P an override applies only where the source has the property." \
		"compression=lz4=local
compression=lz4=local" "$(zxfer_property_test_derive "compression=lz4=local" "copies=2" 1 filesystem)"
}

test_plan_property_changes_preserves_required_create_props_when_transfer_all_disabled() {
	assertEquals "compression=lz4=override,casesensitivity=sensitive=local,normalization=formD=local,utf8only=on=local
compression=lz4=override,casesensitivity=sensitive=local,normalization=formD=local,utf8only=on=local" \
		"$(zxfer_property_test_derive \
			"compression=off=local,casesensitivity=sensitive=local,normalization=formD=local,utf8only=on=local,quota=1G=local" \
			"compression=lz4" 0 filesystem)"
}

test_plan_property_changes_uses_required_create_override_for_creation() {
	assertEquals "casesensitivity=insensitive=override
casesensitivity=insensitive=override" \
		"$(zxfer_property_test_derive "casesensitivity=sensitive=local,compression=off=local" \
			"casesensitivity=insensitive" 0 filesystem)"
}

test_plan_property_changes_uses_explicit_override_for_inherited_creation() {
	assertEquals "An override of an inherited source property is applied but is not a creation-time property." \
		"compression=off=local,atime=off=override
compression=off=local" \
		"$(zxfer_property_test_derive "compression=off=local,atime=on=inherited" "atime=off" 1 filesystem)"
}

test_plan_property_changes_transfer_all_keeps_sources_and_volume_refreservation() {
	assertEquals "compression=lz4=local,quota=8G=override,refreservation=4G=received
compression=lz4=local,quota=8G=override,refreservation=4G=received" \
		"$(zxfer_property_test_derive "compression=lz4=local,quota=1G=local,refreservation=4G=received" \
			"quota=8G" 1 volume)"
}

test_plan_property_changes_escapes_override_values_and_literal_commas() {
	assertEquals "user:note=a%3Db%2Cc%3B%25=override
user:note=a%3Db%2Cc%3B%25=override" \
		"$(zxfer_property_test_derive "" 'user:note=a=b\,c;%' 0 filesystem)"
}

test_plan_property_changes_preserves_literal_backslashes_in_overrides_and_source_values() {
	assertEquals 'user:path=C:\new=override,user:other=D:\temp=local
user:path=C:\new=override,user:other=D:\temp=local' \
		"$(zxfer_property_test_derive 'user:path=x=local,user:other=D:\temp=local' 'user:path=C:\new' 1 filesystem)"
}

test_plan_property_changes_skips_volume_only_properties_for_filesystems() {
	assertEquals "compression=lz4=local
compression=lz4=local" \
		"$(zxfer_property_test_derive "compression=lz4=local,volblocksize=8192=-,volthreading=on=default" "" 1 filesystem)"
	assertEquals "volblocksize=8192=-" \
		"$(zxfer_property_test_derive "volblocksize=8192=-" "" 1 volume)"
}

test_plan_property_changes_filters_readonly_ignored_and_unsupported_entries_with_warnings() {
	(
		g_option_v_verbose=1
		zxfer_property_test_derive \
			"mountpoint=/mnt=local,compression=lz4=local,atime=off=local,user:note=a%2Cb=local,checksum=sha256=local" \
			"mountpoint=/other" 1 filesystem "mountpoint,readonly" "atime" "user:note,checksum" \
			2>"$TEST_TMPDIR/derive_filter.err"
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

test_plan_property_changes_without_a_destination_plans_no_diff() {
	zxfer_plan_property_changes "compression=lz4=local" "" 1 filesystem "" "" ""
	assertEquals "compression=lz4=local" "$g_zxfer_plan_override_pvs_result"
	assertEquals "<><><><>" "<$g_zxfer_plan_dest_pvs_result><$g_zxfer_plan_initial_set_result><$g_zxfer_plan_child_set_result><$g_zxfer_plan_inherit_result>"
}

test_plan_property_changes_reports_awk_failures_with_its_results_cleared() {
	set +e
	output=$(
		(
			g_zxfer_plan_override_pvs_result="stale"
			g_cmd_awk="$TEST_TMPDIR/missing-awk"
			zxfer_throw_error() {
				printf '%s|override=<%s>\n' "$1" "$g_zxfer_plan_override_pvs_result"
				exit 1
			}
			zxfer_plan_property_changes "compression=lz4=local" "" 1 filesystem "" "" "" 2>/dev/null
		)
	)
	status=$?
	assertEquals 1 "$status"
	assertEquals "Failed to plan dataset properties.|override=<>" "$output"
}

test_plan_property_changes_publishes_nothing_when_the_plan_output_is_cut_short() {
	l_cut_short_awk="$TEST_TMPDIR/cut-short-awk"
	cat >"$l_cut_short_awk" <<'EOF'
#!/bin/sh
printf '%s\n' "compression=lz4=local"
EOF
	chmod 755 "$l_cut_short_awk"
	set +e
	output=$(
		(
			g_cmd_awk=$l_cut_short_awk
			zxfer_throw_error() {
				printf '%s|override=<%s>|inherit=<%s>\n' "$1" \
					"$g_zxfer_plan_override_pvs_result" "$g_zxfer_plan_inherit_result"
				exit 1
			}
			zxfer_plan_property_changes "compression=lz4=local" "" 1 filesystem "" "" "" \
				"compression=off=local"
		)
	)
	status=$?
	assertEquals 1 "$status"
	assertEquals "An awk that exits 0 without the end marker must not publish a partial plan." \
		"Failed to plan dataset properties.|override=<>|inherit=<>" "$output"
}

################################################################################
# PLAN: DIFF
################################################################################

# Purpose: Plan against an existing destination and print the initial-set,
# child-set and inherit lists.
# Usage: zxfer_property_test_diff TRANSFER_ALL SOURCE_PVS OVERRIDE_LIST DEST_PVS
# [READONLY_CSV] [IGNORE_CSV]; OVERRIDE_LIST is already serialized.
zxfer_property_test_diff() {
	zxfer_plan_property_changes "$2" "$3" "$1" filesystem "${5:-}" "${6:-}" "" "$4"
	printf '%s\n%s\n%s\n' "$g_zxfer_plan_initial_set_result" "$g_zxfer_plan_child_set_result" "$g_zxfer_plan_inherit_result"
}

test_plan_property_changes_rejects_must_create_mismatches_on_filesystems_only() {
	set +e
	output=$(
		(
			zxfer_throw_error_with_usage() {
				printf '%s\n' "$1"
				exit 1
			}
			zxfer_property_test_diff 1 "casesensitivity=mixed=local" "" "casesensitivity=sensitive=local"
		)
	)
	status=$?
	assertEquals 1 "$status"
	assertContains "$output" "The property \"casesensitivity\" may only be set"
	zxfer_plan_property_changes "casesensitivity=mixed=local" "" 1 volume "" "" "" \
		"casesensitivity=sensitive=local"
	assertEquals "A volume has no creation-time properties to enforce." \
		"casesensitivity=mixed" "$g_zxfer_plan_initial_set_result"
}

test_plan_property_changes_warns_about_unsupported_properties_before_a_must_create_failure() {
	set +e
	(
		g_option_v_verbose=1
		zxfer_throw_error_with_usage() {
			printf 'usage error: %s\n' "$1" >&2
			exit 1
		}
		zxfer_plan_property_changes "casesensitivity=mixed=local,overlay=on=local" "" 1 filesystem \
			"" "" "overlay" "casesensitivity=sensitive=local"
	) 2>"$TEST_TMPDIR/must_create_warning.err"
	assertEquals 1 "$?"
	assertEquals "The -U warnings (one per list) come first." \
		"Destination does not support property overlay=on
Destination does not support property overlay=on" \
		"$(sed -n 1,2p "$TEST_TMPDIR/must_create_warning.err")"
	assertContains "$(sed -n 3p "$TEST_TMPDIR/must_create_warning.err")" \
		"usage error: The property \"casesensitivity\" may only be set"
}

test_plan_property_changes_sets_local_value_when_destination_source_is_inherited() {
	assertEquals "compression=lz4
compression=lz4" "$(zxfer_property_test_diff 1 "compression=lz4=local" "" "compression=lz4=inherited")"
}

test_plan_property_changes_inherits_value_when_destination_is_local_but_source_is_not() {
	assertEquals "

compression=lz4" "$(zxfer_property_test_diff 1 "compression=lz4=inherited" "" "compression=lz4=local")"
}

test_plan_property_changes_treats_overrides_as_parent_sets() {
	assertEquals "

checksum=sha256" "$(zxfer_property_test_diff 0 "" "checksum=sha256=override" "checksum=sha256=local")"
	assertEquals "checksum=sha256

checksum=sha256" "$(zxfer_property_test_diff 0 "" "checksum=sha256=override" "checksum=fletcher4=local")"
}

test_plan_property_changes_does_not_stamp_matching_default_noninheritable_values() {
	assertEquals "" "$(zxfer_property_test_diff 1 "quota=none=default" "" "quota=none=default")"
}

test_plan_property_changes_sets_changed_noninheritable_values_locally() {
	assertEquals "quota=1G
quota=1G" "$(zxfer_property_test_diff 1 "quota=1G=received" "" "quota=none=default")"
}

test_plan_property_changes_inherits_missing_inheritable_override_properties() {
	assertEquals "compression=lz4

compression=lz4" "$(zxfer_property_test_diff 0 "" "compression=lz4=override" "")"
}

test_plan_property_changes_skips_must_create_properties_and_multiple_entries() {
	l_diff_source="casesensitivity=sensitive=-,compression=lz4=local,atime=off=received"
	l_diff_dest="casesensitivity=sensitive=-,compression=off=local,atime=on=local"
	assertEquals "compression=lz4,atime=off
compression=lz4
atime=off" "$(zxfer_property_test_diff 1 "$l_diff_source" "" "$l_diff_dest")"
}

test_plan_property_changes_preserves_literal_backslashes() {
	assertEquals 'user:path=C:\new
user:path=C:\new' "$(zxfer_property_test_diff 1 'user:path=C:\new=local' "" 'user:path=C:\old=local')"
}

test_plan_property_changes_filters_the_destination_list_before_diffing() {
	zxfer_plan_property_changes "compression=lz4=local,readonly=off=local" "readonly=on=override" 1 \
		filesystem "readonly,mountpoint" "atime" "" \
		"compression=off=local,mountpoint=/x=local,atime=on=local,readonly=off=local"
	assertEquals "Readonly and -I entries leave the destination list." \
		"compression=off=local" "$g_zxfer_plan_dest_pvs_result"
	assertEquals "A filtered destination entry reads as absent." \
		"compression=lz4,readonly=on" "$g_zxfer_plan_initial_set_result"
}

test_plan_property_changes_ignores_an_exported_unsupported_list() {
	set +e
	output=$(
		(
			ZXFER_AWK_UNSUPPORTED_LIST=casesensitivity,compression
			export ZXFER_AWK_UNSUPPORTED_LIST
			zxfer_throw_error_with_usage() {
				printf 'usage=%s\n' "$1"
				exit 1
			}
			zxfer_plan_property_changes "compression=lz4=local" "" 1 filesystem "readonly" "" "" \
				"compression=off=local,readonly=off=local"
			printf 'override=%s\n' "$g_zxfer_plan_override_pvs_result"
			printf 'dest=%s\n' "$g_zxfer_plan_dest_pvs_result"
			printf 'child=%s\n' "$(zxfer_filter_child_creation_overrides_for_parent \
				"compression=lz4=override" "compression=gzip=local" "")"
			zxfer_plan_property_changes "" "casesensitivity=insensitive=override" 0 filesystem "" "" "" \
				"casesensitivity=sensitive=-"
			printf 'must-create check skipped\n'
		)
	)
	status=$?
	assertEquals "The must-create mismatch must still fail." 1 "$status"
	assertContains "$output" "override=compression=lz4=local"
	assertContains "$output" "dest=compression=off=local"
	assertContains "$output" "child=compression=lz4=override"
	assertContains "$output" "usage=The property \"casesensitivity\" may only be set"
	assertNotContains "$output" "must-create check skipped"
}
