#!/bin/sh
# Property transfer fragment: the -o reader (syntax, the repeated-property
# rule, encoding), the initial-source -o check, and the plan's fail-closed
# ends. The plan's rules are pinned black-box, as zfs set, inherit and create
# argv, in tests/test_contract_properties.sh. Run by
# tests/test_zxfer_property_transfer.sh.
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
		zxfer_get_temp_file() { return 37; }
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
		g_zxfer_property_input_file=$TEST_TMPDIR
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
		zxfer_ensure_runtime_artifact_file() { return 43; }
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
