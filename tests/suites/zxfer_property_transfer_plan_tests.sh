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

# A plan that awk cannot run, or that ends before its marker, fails closed
# with none of its results published (the fault injector cannot fail awk).
test_plan_property_changes_publishes_nothing_when_awk_fails_or_stops_short() {
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

	l_cut_short_awk="$TEST_TMPDIR/cut-short-awk"
	cat >"$l_cut_short_awk" <<'EOF'
#!/bin/sh
printf '%s\n' "compression=lz4=local"
EOF
	chmod 755 "$l_cut_short_awk"
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
