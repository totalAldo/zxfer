#!/bin/sh
#
# Black-box pins for the property pass (-P): the real ./zxfer launcher runs
# over the canned zfs from tests/helpers/blackbox.sh, behind a wrapper that
# records every create, set and inherit argv one argument per line, so
# argument boundaries are visible.
#
# Pins:
#   test_soh_property_value_stays_one_set_argument
#   → a source value holding \001 and "sharenfs=rw" reaches `zfs set` as one
#     argument, locally and over -T.
#   test_newline_property_value_stays_one_set_argument
#   → a source value holding a newline and "readonly=off" reaches `zfs set` as
#     one argument, locally and over -T.
#   test_record_shaped_property_value_is_set_whole_and_injects_nothing
#   → a source value shaped like a second `zfs get` record reaches `zfs set`
#     whole, its tail never becomes a property, and only that property is
#     re-read alone.
#   test_dash_named_properties_are_read_alone_after_end_of_options
#   → user properties named -x:y and -x:m (one after a multi-line value, one
#     multi-line) are read alone with -- before the name, under -P, -o, -O
#     and -T, so zfs never parses the name as an option.
#   test_recursive_read_race_residual_forges_a_recreated_dataset
#   → pins the documented Low race (KNOWN_ISSUES.md): a dataset absent from
#     the recursive value views but listed in the name list takes its records
#     from the previous value's continuation lines, a native one included.
#   test_user_property_removed_before_its_lone_read_is_left_out
#   → a user property removed between the name list and its lone re-read
#     (which zfs answers with source "-") is left out: the destination keeps
#     its own value and the -k row has none. Uses the argv-fuzz fake zfs.
#   test_soh_property_value_stays_one_create_argument
#   → a creation value holding "\001-o\001mountpoint=..." reaches
#     `zfs create` as one -o argument.
#   test_property_pass_reads_each_tree_once_with_type_filter
#   → -P -R reads each side with one recursive machine/human/skeleton triple
#     filtered to filesystems and volumes, and never falls back to
#     per-dataset reads.
#
# shellcheck disable=SC1090,SC2034,SC2154

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

# shellcheck source=tests/helpers/blackbox.sh
. "$TESTS_DIR/helpers/blackbox.sh"

# shellcheck source=tests/helpers/argv_fuzz.sh
. "$TESTS_DIR/helpers/argv_fuzz.sh"

# Purpose: Put an argv recorder in front of the canned zfs.
# Usage: blackbox_properties_record_mutation_argv; each create, set or
# inherit appends "ARGV <count>" and then one "[argument]" line per argument
# to ARGV_LOG.
blackbox_properties_record_mutation_argv() {
	ARGV_LOG="$CASE_DIR/argv.log"
	: >"$ARGV_LOG"
	mv "$MOCKBIN_DIR/zfs" "$MOCKBIN_DIR/zfs.canned" ||
		fail "Unable to stage the canned zfs behind the argv recorder."
	cat >"$MOCKBIN_DIR/zfs" <<EOF
#!/bin/sh
case "\${1:-}" in
create | set | inherit)
	{
		printf 'ARGV %s\n' "\$#"
		for recorded_arg in "\$@"; do
			printf '[%s]\n' "\$recorded_arg"
		done
	} >>"$ARGV_LOG"
	;;
esac
exec "$MOCKBIN_DIR/zfs.canned" "\$@"
EOF
	chmod +x "$MOCKBIN_DIR/zfs"
}

# Purpose: Seed STATE_DIR with default property rows plus one local user
# property on the source root whose value holds \001 and an extra
# assignment.
# Usage: blackbox_properties_add_soh_root_fixture NAME
blackbox_properties_add_soh_root_fixture() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" "$1"
	l_soh_rows=$(planning_property_default_rows)
	l_soh_root_rows=$(printf '%s\ncom.x:note\ta\001sharenfs=rw\tlocal' "$l_soh_rows")
	planning_add_property_fixtures_for_rows "$l_soh_root_rows" "$l_soh_rows" \
		"$l_soh_rows" "$l_soh_rows"
	blackbox_properties_record_mutation_argv
}

test_soh_property_value_stays_one_set_argument() {
	l_expected_set=$(printf 'ARGV 3\n[set]\n[com.x:note=a\001sharenfs=rw]\n[%s]' \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT")

	blackbox_properties_add_soh_root_fixture soh_set
	planning_run_property_pass -P
	assertEquals "the \\001 value must reach zfs set as one argument" \
		"$l_expected_set" "$(cat "$ARGV_LOG")"

	blackbox_properties_add_soh_root_fixture soh_set_remote
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_property_pass -T localhost -P
	assertEquals "the \\001 value must reach a -T zfs set as one argument" \
		"$l_expected_set" "$(cat "$ARGV_LOG")"
}

# Purpose: Print property values raw, as zfs get prints them, in STATE_DIR
# fixture files whose values spell TAB as @TAB@ and LF as @LF@.
# Usage: blackbox_properties_expand_values FILE...
blackbox_properties_expand_values() {
	for l_expand_list in "$@"; do
		if ! awk '{ gsub(/@TAB@/, "\t"); gsub(/@LF@/, "\n"); print }' \
			"$STATE_DIR/$l_expand_list" >"$STATE_DIR/$l_expand_list.new" ||
			! mv "$STATE_DIR/$l_expand_list.new" "$STATE_DIR/$l_expand_list"; then
			fail "Unable to expand the values in $l_expand_list."
		fi
	done
}

# Purpose: Seed STATE_DIR with default property rows plus one local user
# property com.x:note on the source root whose value spans lines, printed raw
# as zfs get prints it, and answer the lone reads of that property.
# Usage: blackbox_properties_add_multiline_root_fixture NAME VALUE; VALUE
# spells TAB as @TAB@ and LF as @LF@.
blackbox_properties_add_multiline_root_fixture() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" "$1"
	l_multiline_rows=$(planning_property_default_rows)
	l_multiline_root_rows=$(printf '%s\ncom.x:note\t%s\tlocal' "$l_multiline_rows" "$2")
	planning_add_property_fixtures_for_rows "$l_multiline_root_rows" "$l_multiline_rows" \
		"$l_multiline_rows" "$l_multiline_rows"
	printf 'com.x:note\t%s\tlocal\n' "$2" >"$STATE_DIR/src_note.list"
	blackbox_properties_expand_values src_props_tree.list src_props_root.list src_note.list
	printf '%s\tsrc_note.list\t0\n' \
		"get -Hpo property,value,source -- com.x:note $ZXFER_MOCKBIN_SOURCE_ROOT" \
		"get -Ho property,value,source -- com.x:note $ZXFER_MOCKBIN_SOURCE_ROOT" \
		>>"$STATE_DIR/manifest" || fail "Unable to append the lone-read rules."
	blackbox_properties_record_mutation_argv
}

test_newline_property_value_stays_one_set_argument() {
	l_expected_set=$(printf 'ARGV 3\n[set]\n[com.x:note=a\nreadonly=off]\n[%s]' \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT")

	blackbox_properties_add_multiline_root_fixture newline_set 'a@LF@readonly=off'
	planning_run_property_pass -P
	assertEquals "a newline value must reach zfs set as one argument" \
		"$l_expected_set" "$(cat "$ARGV_LOG")"

	blackbox_properties_add_multiline_root_fixture newline_set_remote 'a@LF@readonly=off'
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_property_pass -T localhost -P
	assertEquals "a newline value must reach a -T zfs set as one argument" \
		"$l_expected_set" "$(cat "$ARGV_LOG")"
}

test_record_shaped_property_value_is_set_whole_and_injects_nothing() {
	l_expected_set=$(printf 'ARGV 3\n[set]\n[com.x:note=x\tlocal\ncom.x:fake\ty]\n[%s]' \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT")

	blackbox_properties_add_multiline_root_fixture record_shaped \
		'x@TAB@local@LF@com.x:fake@TAB@y'
	planning_run_property_pass -P
	assertEquals "a value shaped like two records must reach zfs set whole" \
		"$l_expected_set" "$(cat "$ARGV_LOG")"
	assertFalse "the record-shaped tail must never become a property" \
		"grep -q 'com.x:fake=' '$ARGV_LOG'"
	assertEquals "the ambiguous source root is re-read live, one property alone" \
		"get -Ho property,value,source -- com.x:note $ZXFER_MOCKBIN_SOURCE_ROOT
get -Hpo property,value,source -- com.x:note $ZXFER_MOCKBIN_SOURCE_ROOT" \
		"$(grep '^get .*com.x:note' "$ZFS_LOG")"
}

# Purpose: Seed STATE_DIR so both roots hold hn:desc (two lines), then -x:y
# (one line) and -x:m (two lines), printed raw as zfs get prints them, and
# answer their lone reads as zfs's getopt does: a property operand that
# starts with "-" is an invalid option unless -- ends the options first.
# hn:desc answers with or without --, so code that omits -- reaches -x:y
# and fails there the way zfs does.
# Usage: blackbox_properties_add_dash_named_fixture NAME
blackbox_properties_add_dash_named_fixture() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" "$1"
	: >"$ZFS_LOG"
	l_dash_rows=$(planning_property_default_rows)
	l_dash_user_rows=$(printf '%s\t%s\t%s\n' hn:desc 'Backups of@LF@the web tier' local \
		-x:y one local -x:m 'line1@LF@line2' local)
	l_dash_root_rows=$(printf '%s\n%s' "$l_dash_rows" "$l_dash_user_rows")
	planning_add_property_fixtures_for_rows "$l_dash_root_rows" "$l_dash_rows" \
		"$l_dash_root_rows" "$l_dash_rows"
	l_dash_index=0
	for l_dash_property in hn:desc -x:y -x:m; do
		l_dash_index=$((l_dash_index + 1))
		printf '%s\n' "$l_dash_user_rows" |
			awk -F'\t' -v name="$l_dash_property" '$1 == name' >"$STATE_DIR/lone_$l_dash_index.list"
		blackbox_properties_expand_values "lone_$l_dash_index.list"
		l_dash_operand="-- $l_dash_property"
		[ "$l_dash_property" = hn:desc ] && l_dash_operand="*hn:desc"
		for l_dash_root in "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"; do
			printf '%s\tlone_%s.list\t0\n' \
				"get -Hpo property,value,source $l_dash_operand $l_dash_root" "$l_dash_index" \
				"get -Ho property,value,source $l_dash_operand $l_dash_root" "$l_dash_index"
		done
	done >>"$STATE_DIR/manifest" || fail "Unable to append the lone-read rules."
	blackbox_properties_expand_values src_props_tree.list src_props_root.list \
		dst_props_tree.list dst_props_root.list
	printf "invalid option 'x'\n" >"$STATE_DIR/invalid_option.list"
	printf '%s\tinvalid_option.list\t2\n' "get -H*o property,value,source -[!-]*" \
		>>"$STATE_DIR/manifest" || fail "Unable to append the getopt rule."
}

test_dash_named_properties_are_read_alone_after_end_of_options() {
	for l_dash_mode in P o O T; do
		blackbox_properties_add_dash_named_fixture "dash_$l_dash_mode"
		case $l_dash_mode in
		P) planning_run_property_pass -P ;;
		o) planning_run_property_pass -o compression=gzip ;;
		O | T)
			planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
				fail "Unable to write socket-aware mock ssh."
			PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
				planning_run_property_pass "-$l_dash_mode" localhost -P
			;;
		esac
		for l_dash_root in "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"; do
			assertEquals "-$l_dash_mode: $l_dash_root reads -x:y and -x:m alone in both views, after --" \
				4 "$(grep '^get -H[p]*o property,value,source -- -x:[ym] ' "$ZFS_LOG" |
					awk -v root="$l_dash_root" '$NF == root' | sort -u | wc -l | tr -d ' ')"
		done
		assertEquals "-$l_dash_mode: no property operand may precede --; zfs log: $(cat "$ZFS_LOG")" \
			0 "$(grep -c '^get -H[p]*o property,value,source -[^-]' "$ZFS_LOG")"
		assertEquals "-$l_dash_mode: a matching or override-only pass never mutates a dash-named property" \
			0 "$(grep '^MUTATE ' "$ZFS_LOG" | grep -c -- '-x:')"
	done
}

# The source's recursive value views lack child2 (destroyed before them) and
# its name list holds it again (recreated before it). child1's last value
# ends with lines shaped like child2's records, readonly=on among them.
# This pins the residual as documented; hardening the prefetch turns it around.
test_recursive_read_race_residual_forges_a_recreated_dataset() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" race_residual
	l_rows=$(planning_property_default_rows)
	planning_add_property_fixtures_for_rows "$l_rows" "$l_rows" "$l_rows" "$l_rows"
	l_child1="$ZXFER_MOCKBIN_SOURCE_ROOT/child1"
	l_child2="$ZXFER_MOCKBIN_SOURCE_ROOT/child2"
	l_tab=$(printf '\t')
	# com.x:note's value is "v<TAB>local" and then child2's records, the last
	# one without its source: zfs ends the value with a TAB and "local".
	l_forged=$(planning_property_rows_with "$l_rows" readonly on local |
		awk -v dataset="$l_child2" '{ printf "@LF@%s\t%s", dataset, $0 }')
	l_forged="$l_child1${l_tab}com.x:note${l_tab}v${l_tab}local${l_forged%"$l_tab"*}${l_tab}local"
	if ! awk -F'\t' -v child1="$l_child1" -v child2="$l_child2" -v forged="$l_forged" '
		$1 == child2 { next }
		{ print }
		$1 == child1 && $2 == "utf8only" { print forged }' \
		"$STATE_DIR/src_props_tree.list" >"$STATE_DIR/src_props_tree.list.new" ||
		! mv "$STATE_DIR/src_props_tree.list.new" "$STATE_DIR/src_props_tree.list"; then
		fail "Unable to drop child2 from the source value views."
	fi
	if ! awk -F'\t' -v child1="$l_child1" '{ print }
		$1 == child1 && $2 == "utf8only" { print child1 "\tcom.x:note" }' \
		"$STATE_DIR/src_props_tree.names" >"$STATE_DIR/src_props_tree.names.new" ||
		! mv "$STATE_DIR/src_props_tree.names.new" "$STATE_DIR/src_props_tree.names"; then
		fail "Unable to list child1's com.x:note."
	fi
	blackbox_properties_expand_values src_props_tree.list

	planning_run_property_pass -P

	assertEquals "the forged native value and the cut value reach zfs set; zfs log: $(cat "$ZFS_LOG")" \
		"MUTATE set com.x:note=v $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1
MUTATE set readonly=on $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2" "$(grep '^MUTATE ' "$ZFS_LOG")"
}

# Purpose: Run ./zxfer -k -N tank/src backup against the argv-fuzz fake zfs
# (tests/helpers/argv_fuzz.awk) in CASE_DIR/NAME. tank/src holds user:car
# (two lines, so every later property is read alone) and user:k=real;
# backup/src holds user:k=old. With RACE=yes, user:k is removed from tank/src
# just before its lone -Ho read, after the name list listed it.
# Usage: blackbox_properties_run_removed_property_race NAME no|yes; publishes
# RACE_DIR and RACE_STATUS.
blackbox_properties_run_removed_property_race() {
	RACE_DIR="$CASE_DIR/$1"
	if ! mkdir -p "$RACE_DIR/state" "$RACE_DIR/tmp" || ! chmod 700 "$RACE_DIR/tmp"; then
		fail "Unable to create $RACE_DIR."
	fi
	# Model lines are TAB-separated with values escaped (\012 is LF).
	{
		printf 'D\t%s\t%s\n' tank 500000000001 tank/src 500000000002 \
			backup 500000000003 backup/src 500000000004
		printf 'S\t%s\ts1\t100000000001\t1700000000\n' tank/src backup/src
		printf 'P\t%s\t%s\t%s\tlocal\n' tank/src user:car 'multi\012line' \
			tank/src user:k real backup/src user:k old
	} >"$RACE_DIR/state/model" || fail "Unable to write the model."
	: >"$RACE_DIR/state/argv.log"
	: >"$RACE_DIR/state/violations"
	if [ "$2" = yes ]; then
		printf 'get\t-Ho\tproperty,value,source\t--\tuser:k\ttank/src\nX\ttank/src\tuser:k\n' \
			>"$RACE_DIR/state/race" || fail "Unable to write the race."
	fi
	argv_fuzz_write_mockbin "$RACE_DIR/mockbin" "$RACE_DIR/state" ||
		fail "Unable to write the argv-fuzz mock bin."
	RACE_STATUS=0
	(
		TMPDIR=$RACE_DIR/tmp
		PATH=$(zxfer_mockbin_secure_path_env "$RACE_DIR/mockbin")
		ZXFER_BACKUP_DIR=$RACE_DIR/backup
		export TMPDIR PATH ZXFER_BACKUP_DIR
		zxfer_mockbin_run_zxfer "$RACE_DIR/mockbin" "$RACE_DIR/state" "" \
			-k -N tank/src backup
	) >"$RACE_DIR/stdout" 2>"$RACE_DIR/stderr" </dev/null || RACE_STATUS=$?
}

# zfs answers the lone read of a user property removed since the name list
# with "user:k<TAB>-<TAB>-". That must read as absent, as if the read had begun
# after the removal: backup/src keeps its own user:k, never "-", and the -k
# row has no user:k. Without the race user:k=real is set, so the lone read
# is what decides.
test_user_property_removed_before_its_lone_read_is_left_out() {
	for l_race in no yes; do
		blackbox_properties_run_removed_property_race "race_$l_race" "$l_race"
		assertEquals "race=$l_race: zxfer exits 0; stderr: $(cat "$RACE_DIR/stderr")" \
			0 "$RACE_STATUS"
		assertEquals "race=$l_race: the fake zfs saw only whole names" \
			"" "$(cat "$RACE_DIR/state/violations")"
		l_backup_rows=$(find "$RACE_DIR/backup" -type f -name .zxfer_backup_info.v2 \
			-exec cat {} + 2>/dev/null | grep '^\.	' || :)
		assertContains "race=$l_race: the -k row keeps user:car" \
			"$l_backup_rows" "user:car=multi%0Aline=local"
		case $l_race in
		no)
			l_want_set=$(printf 'set\tuser:car=multi\\012line\tuser:k=real\tbackup/src')
			assertContains "race=no: the -k row holds user:k" "$l_backup_rows" "user:k=real=local"
			;;
		yes)
			l_want_set=$(printf 'set\tuser:car=multi\\012line\tbackup/src')
			assertEquals "race=yes: the race removed user:k from tank/src" \
				1 "$(grep -c '^X	tank/src	user:k$' "$RACE_DIR/state/model")"
			assertNotContains "race=yes: the -k row has no user:k" "$l_backup_rows" "user:k="
			assertEquals "race=yes: backup/src keeps user:k=old" \
				"P	backup/src	user:k	old	local" \
				"$(grep '	backup/src	user:k' "$RACE_DIR/state/model")"
			;;
		esac
		assertEquals "race=$l_race: the one property mutation" \
			"$l_want_set" "$(grep -v -e '^get' -e '^list' "$RACE_DIR/state/argv.log")"
	done
}

test_soh_property_value_stays_one_create_argument() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" soh_create
	l_rows=$(planning_property_default_rows)
	l_child_rows=$(printf '%s\ncom.x:note\ta\001-o\001mountpoint=/etc/cron.d\tlocal' "$l_rows")
	planning_add_property_fixtures_for_rows "$l_rows" "$l_child_rows" "$l_rows" "$l_rows"
	l_missing_child="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2"
	for l_fixture in dst_datasets.list dst_snapshots.list dst_props_tree.list; do
		grep -v "^$l_missing_child" "$STATE_DIR/$l_fixture" >"$STATE_DIR/$l_fixture.new" || :
		mv "$STATE_DIR/$l_fixture.new" "$STATE_DIR/$l_fixture" ||
			fail "Unable to drop $l_missing_child from $l_fixture."
	done
	: >"$STATE_DIR/dst_d1_2.list"
	printf "cannot open '%s': dataset does not exist\n" "$l_missing_child" \
		>"$STATE_DIR/missing_child2.list"
	printf 'list -H %s\tmissing_child2.list\t1\n' "$l_missing_child" \
		>>"$STATE_DIR/manifest" || fail "Unable to append missing-child rule."
	blackbox_properties_record_mutation_argv

	planning_run_property_pass -P

	l_expected_create=$(printf '%s\n' 'ARGV 16' '[create]' \
		'[-o]' '[compression=lz4]' '[-o]' '[readonly=off]' '[-o]' '[atime=off]' \
		'[-o]' '[casesensitivity=sensitive]' '[-o]' '[normalization=none]' \
		'[-o]' '[utf8only=off]' '[-o]')
	l_expected_create=$(printf '%s\n[com.x:note=a\001-o\001mountpoint=/etc/cron.d]\n[%s]' \
		"$l_expected_create" "$l_missing_child")
	# Keep only the create records: an "ARGV" header line followed by "[create]".
	l_create_argv=$(awk '/^ARGV /{ header = $0; header_line = NR; next }
		NR == header_line + 1 { keep = ($0 == "[create]"); if (keep) print header }
		keep' "$ARGV_LOG")
	assertEquals "the \\001 value must reach zfs create as one -o argument" \
		"$l_expected_create" "$l_create_argv"
	assertFalse "no argument may carry the injected assignment alone" \
		"grep -qx '\\[mountpoint=/etc/cron.d\\]' '$ARGV_LOG'"
}

test_property_pass_reads_each_tree_once_with_type_filter() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" prefetch_filter
	planning_add_property_transfer_fixtures

	planning_run_property_pass -P

	for l_prefetch_root in "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"; do
		for l_prefetch_view in "-Ho name,property" "-Hpo name,property,value,source" \
			"-Ho name,property,value,source"; do
			assertEquals "one $l_prefetch_view tree read of $l_prefetch_root" 1 \
				"$(grep -cx "get -r -t filesystem,volume $l_prefetch_view all $l_prefetch_root" "$ZFS_LOG")"
		done
	done
	assertEquals "the prefetch must answer every dataset; zfs log: $(cat "$ZFS_LOG")" \
		0 "$(grep -c '^get -H[p]*o property[,a-z]* all ' "$ZFS_LOG")"
	planning_assert_no_mutations
}

. "$SHUNIT2_BIN"
