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
#     and -T, so zfs never parses the name as an option; under -O and -T
#     every read of the remote side, live and lone reads included, crosses
#     ssh and no read of the local side does.
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
#   → -P -R and -o -R read each side with one recursive machine/human/skeleton
#     triple filtered to filesystems and volumes, never fall back to
#     per-dataset reads, and never probe a type or zvol size the lists hold.
#   test_repeated_override_property_is_a_usage_error_before_any_zfs_call
#   → a property named twice in -o, with the same or another value, exits 2
#     with a usage report from CLI validation before zxfer runs any zfs
#     command, as zfs itself refuses a property given twice.
#   test_override_missing_on_the_source_fails_before_the_destination_is_touched
#   → an -o property the source root lacks exits 2 with a usage report once
#     the source is read, before any destination property read, probe or
#     change.
#   test_property_pass_applies_each_plan_rule_with_one_set_and_one_inherit_per_property
#   → the plan's set and inherit rules (local, inherited, read-only,
#     volume-only, noninheritable, creation-time), one `zfs set` per dataset
#     and one `zfs inherit` per property; exported ZXFER_AWK_* lists change
#     nothing.
#   test_parent_is_read_again_once_after_its_set_and_children_it_does_not_match_are_set
#   → a set drops the dataset's cached view: the next child reads it again
#     once, the child after reuses it, and an inherit the parent does not
#     match becomes a local set.
#   test_override_values_reach_zfs_set_whole_in_order_locally_and_over_T
#   → -o values with escaped commas, quotes, backslashes and command
#     substitutions reach `zfs set` whole, in -o order (source order with
#     -P), locally and over -T, and children inherit them.
#   test_missing_children_are_created_with_their_creation_properties_and_recorded
#   → a missing volume is created with its size and refreservation, a missing
#     filesystem with its creation list minus the -o value its parent holds;
#     both are seeded with -F, reconciled from a fresh destination read, and
#     recorded by -k.
#   test_override_alone_creates_a_missing_child_with_the_list_and_creation_time_properties
#   → without -P a missing child is created with the -o list and the
#     creation-time properties only.
#   test_destination_created_after_discovery_is_diffed_not_created
#   → a destination the listing lacks but a live probe finds is diffed.
#   test_missing_destination_root_is_created_with_its_whole_list_after_its_parent
#   → a missing destination root is created after its parent (-p) with its
#     whole list, and its children with their creation lists.
#   test_missing_creation_time_properties_are_read_back_and_enforced
#   → creation-time properties `zfs get all` omits are read with one
#     comma-list get, else one at a time (an inapplicable one left out), and
#     a differing one is refused before any change.
#   test_skip_unsupported_probes_only_missing_names_and_warns_about_each_skip
#   test_skip_unsupported_keeps_volume_and_filesystem_skips_apart
#   → -U probes only the names a same-type destination lacks, skips what it
#     rejects (per dataset type), and -v warns about each skip.
#   test_freebsd_destination_never_sets_its_read_only_properties
#   → a FreeBSD destination (uname) never gets aclmode set.
#   test_verbose_shows_decoded_lists_and_each_command
#   → -v prints the decoded set and inherit lists and each command (the ssh
#     line over -T), escaping control bytes, also in the -V list dumps.
#   test_property_reads_fall_back_to_one_dataset_at_a_time
#   → -N and a failed tree read read each dataset alone, the tree once.
#   test_each_yield_pass_reads_properties_again
#   → each -Y pass reads properties afresh.
#   test_property_read_failures_stop_with_the_dataset_and_the_zfs_diagnostic
#   test_property_change_failures_stop_at_the_failing_command
#   test_skip_unsupported_scan_failures_stop_before_any_change
#   → each failed read, probe, set, inherit or create stops the run with its
#     status and one report naming the stage, the step and the diagnostic.
#   test_restore_row_missing_or_repeated_for_a_dataset_is_a_usage_error
#   → -e refuses a dataset whose backup row is missing or repeated.
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
# Usage: blackbox_properties_add_multiline_root_fixture NAME VALUE [STATE];
# VALUE spells TAB as @TAB@ and LF as @LF@; STATE is the fixture state to
# clone (noop by default).
blackbox_properties_add_multiline_root_fixture() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/${3:-noop}" "$1"
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

# Purpose: Fail unless every zfs get naming the remote side (-O: the source,
# -T: the destination) crossed ssh, the live and lone reads included, and no
# get naming the other side did. Remote argv reaches SSH_LOG as quoted
# tokens, so quotes are stripped before matching.
# Usage: blackbox_properties_assert_reads_routed O|T
blackbox_properties_assert_reads_routed() {
	l_routed_remote=$ZXFER_MOCKBIN_DEST_ROOT
	l_routed_local=$ZXFER_MOCKBIN_SOURCE_ROOT
	if [ "$1" = O ]; then
		l_routed_remote=$ZXFER_MOCKBIN_SOURCE_ROOT
		l_routed_local=$ZXFER_MOCKBIN_DEST_ROOT
	fi
	l_routed_ssh=$(tr -d "'" <"$SSH_LOG")
	l_routed_gets=$(grep '^get ' "$ZFS_LOG" | grep -F -e " $l_routed_remote" || :)
	assertNotNull "-$1: the remote side's properties must be read" "$l_routed_gets"
	while IFS= read -r l_routed_get; do
		[ -n "$l_routed_get" ] || continue
		case $l_routed_ssh in
		*"$l_routed_get"*) ;;
		*) fail "-$1: this read ran locally: $l_routed_get" ;;
		esac
	done <<EOF
$l_routed_gets
EOF
	assertEquals "-$1: no read of the local side may cross ssh" 0 \
		"$(printf '%s\n' "$l_routed_ssh" | grep -c -e "get .* $l_routed_local" || :)"
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
			SSH_LOG="$CASE_DIR/ssh_$l_dash_mode.log"
			: >"$SSH_LOG"
			MOCK_SSH_LOG=$SSH_LOG
			export MOCK_SSH_LOG
			PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
				planning_run_property_pass "-$l_dash_mode" localhost -P
			unset MOCK_SSH_LOG
			blackbox_properties_assert_reads_routed "$l_dash_mode"
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
	for l_prefetch_mode in P o; do
		planning_setup_env
		: >"$ZFS_LOG"
		planning_clone_state "$FIXTURE_DIR/noop" "prefetch_filter_$l_prefetch_mode"
		planning_add_property_transfer_fixtures

		if [ "$l_prefetch_mode" = P ]; then
			planning_run_property_pass -P
			planning_assert_no_mutations
		else
			planning_run_property_pass -o compression=lz4
		fi

		for l_prefetch_root in "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"; do
			for l_prefetch_view in "-Ho name,property" "-Hpo name,property,value,source" \
				"-Ho name,property,value,source"; do
				assertEquals "-$l_prefetch_mode: one $l_prefetch_view tree read of $l_prefetch_root" 1 \
					"$(grep -cx "get -r -t filesystem,volume $l_prefetch_view all $l_prefetch_root" "$ZFS_LOG")"
			done
		done
		assertEquals "-$l_prefetch_mode: the prefetch must answer every dataset; zfs log: $(cat "$ZFS_LOG")" \
			0 "$(grep -c '^get -H[p]*o property[,a-z]* all ' "$ZFS_LOG")"
		assertEquals "-$l_prefetch_mode: type and volsize come from the lists, never a probe" \
			0 "$(grep -c '^get -Hpo value ' "$ZFS_LOG")"
	done
}

# Purpose: Fail unless stderr holds one usage-class failure report with the
# given stage and message.
# Usage: blackbox_properties_assert_usage_report STAGE MESSAGE
blackbox_properties_assert_usage_report() {
	for l_usage_line in "failure_class: usage" "failure_stage: $1" "message: $2" \
		"Error: $2"; do
		grep -Fqx "$l_usage_line" "$CASE_DIR/zxfer.stderr" ||
			fail "Missing failure report line: $l_usage_line
stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	done
	assertEquals "exactly one failure report" \
		1 "$(grep -c '^zxfer: failure report begin$' "$CASE_DIR/zxfer.stderr")"
}

test_repeated_override_property_is_a_usage_error_before_any_zfs_call() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" repeated_override
	planning_add_property_transfer_fixtures
	for l_repeated in compression=lz4,compression=gzip compression=gzip,atime=off,compression=gzip; do
		rm -f "$ZFS_LOG"
		planning_run_zxfer "$STATE_DIR" -P -o "$l_repeated" -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		assertEquals "-o $l_repeated must exit 2" 2 "$?"
		blackbox_properties_assert_usage_report "cli validation" \
			"Duplicate property for -o override: compression."
		assertFalse "-o $l_repeated must stop before any zfs command; zfs log: $(cat "$ZFS_LOG" 2>/dev/null)" \
			"[ -s '$ZFS_LOG' ]"
	done
}

test_override_missing_on_the_source_fails_before_the_destination_is_touched() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" missing_override
	planning_add_property_transfer_fixtures

	planning_run_zxfer "$STATE_DIR" -o compression=gzip,copies=2 -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "an -o property the source lacks must exit 2" 2 "$?"
	blackbox_properties_assert_usage_report "property transfer" \
		"Missing source property for -o override: copies."
	assertTrue "the source properties are read first; zfs log: $(cat "$ZFS_LOG")" \
		"grep -q '^get .* $ZXFER_MOCKBIN_SOURCE_ROOT\$' '$ZFS_LOG'"
	assertEquals "no destination property is read or probed; zfs log: $(cat "$ZFS_LOG")" \
		0 "$(grep -c "^get .* $ZXFER_MOCKBIN_DEST_ROOT" "$ZFS_LOG")"
	planning_assert_no_mutations
	planning_assert_no_send_receive
}

# ---------------------------------------------------------------------------
# Plan, create and apply rules, and the failure of each step. The canned zfs
# never applies a change: a dataset read again after a set, create or inherit
# answers its old rows unless a case gives it rows of its own.

# Purpose: Put manifest rules ahead of every rule in STATE_DIR (the first
# matching rule wins).
# Usage: blackbox_properties_prepend_rules RULE...; each RULE is a whole
# "pattern<TAB>fixture<TAB>status" line.
blackbox_properties_prepend_rules() {
	if ! {
		printf '%s\n' "$@"
		cat "$STATE_DIR/manifest"
	} >"$STATE_DIR/manifest.new" ||
		! mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest"; then
		fail "Unable to prepend manifest rules."
	fi
}

# Purpose: Answer the per-dataset reads of one dataset (both value views and
# the name list) with ROWS, ahead of the shared rules; the recursive listing
# keeps its rows, so this is what a live read finds later.
# Usage: blackbox_properties_answer_live_reads DATASET ROWS [once]; ROWS are
# "property<TAB>value<TAB>source" lines. With once, only the first live read
# gets ROWS; later ones fall through to the rules behind.
blackbox_properties_answer_live_reads() {
	l_live_file="live_$(printf '%s' "$1" | tr '/' '_')${3:+_$3}.list"
	if ! printf '%s\n' "$2" >"$STATE_DIR/$l_live_file" ||
		! cut -f1 "$STATE_DIR/$l_live_file" >"$STATE_DIR/$l_live_file.names"; then
		fail "Unable to write the live rows of $1."
	fi
	blackbox_properties_prepend_rules \
		"get -Hpo property,value,source all $1	$l_live_file	0	${3:-}" \
		"get -Ho property,value,source all $1	$l_live_file	0	${3:-}" \
		"get -Ho property all $1	$l_live_file.names	0	${3:-}"
}

# Purpose: Give one dataset its own rows in its side's recursive listing and
# name list (in place, or appended) and in its per-dataset reads.
# Usage: blackbox_properties_use_rows src|dst DATASET ROWS
blackbox_properties_use_rows() {
	blackbox_properties_answer_live_reads "$2" "$3"
	if ! awk -F'\t' -v dataset="$2" -v rows="$STATE_DIR/$l_live_file" '
		function emit(    line) {
			while ((getline line < rows) > 0)
				print dataset "\t" line
			close(rows)
			emitted = 1
		}
		$1 == dataset { if (!emitted) emit(); next }
		{ print }
		END { if (!emitted) emit() }' "$STATE_DIR/$1_props_tree.list" \
		>"$STATE_DIR/$1_props_tree.list.new" ||
		! mv "$STATE_DIR/$1_props_tree.list.new" "$STATE_DIR/$1_props_tree.list" ||
		! cut -f1,2 "$STATE_DIR/$1_props_tree.list" >"$STATE_DIR/$1_props_tree.names"; then
		fail "Unable to give $2 its own $1 tree rows."
	fi
}

# Purpose: Make one destination child missing: drop it from the destination
# listings, snapshots and property tree, and answer its exact probe with
# zfs's missing-dataset line.
# Usage: blackbox_properties_drop_destination_child child1|child2
blackbox_properties_drop_destination_child() {
	l_drop_dataset="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/$1"
	for l_drop_fixture in dst_datasets.list dst_snapshots.list dst_props_tree.list \
		dst_props_tree.names; do
		if ! awk -F'\t' -v dataset="$l_drop_dataset" '
			$1 == dataset || index($1, dataset "@") == 1 { next }
			{ print }' "$STATE_DIR/$l_drop_fixture" >"$STATE_DIR/$l_drop_fixture.new" ||
			! mv "$STATE_DIR/$l_drop_fixture.new" "$STATE_DIR/$l_drop_fixture"; then
			fail "Unable to drop $l_drop_dataset from $l_drop_fixture."
		fi
	done
	: >"$STATE_DIR/dst_d1_${1#child}.list"
	printf "cannot open '%s': dataset does not exist\n" "$l_drop_dataset" \
		>"$STATE_DIR/missing_$1.list" || fail "Unable to write the missing-dataset line."
	blackbox_properties_prepend_rules "list -H $l_drop_dataset	missing_$1.list	1"
}

# Purpose: Print the MUTATE lines of the zfs log.
# Usage: blackbox_properties_mutations
blackbox_properties_mutations() {
	grep '^MUTATE ' "$ZFS_LOG" || :
}

# One -P pass pins the plan's rules. The root sets, in one `zfs set`, a
# differing local value (checksum, and com.x:path with its backslash), and a
# local value the destination only inherits (compression); it leaves alone a
# read-only property (mountpoint), a matching noninheritable default (quota),
# and a value the source inherits (copies): the initial source is never
# inherited. Each child sets its local values (atime, and readonly, which it
# only inherits on the destination) and a changed noninheritable one (quota)
# in one `zfs set`, then inherits each value the parent provides, one
# `zfs inherit` per property. volblocksize, volume-only but listed for these
# filesystems, is never set, though no destination parent provides it. The
# root's live read after its set answers the set values. Exported ZXFER_AWK_*
# lists must not reach the plan.
test_property_pass_applies_each_plan_rule_with_one_set_and_one_inherit_per_property() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" plan_rules
	l_rows=$(planning_property_default_rows)
	l_src_root=$(
		printf '%s\n' "$l_rows"
		printf '%s\t%s\t%s\n' checksum sha256 local copies 2 'inherited from srcpool' \
			quota none default volblocksize 8192 - com.x:path 'D:\temp' local
	)
	l_dst_root=$(planning_property_rows_with "$l_rows" mountpoint /mnt/elsewhere local)
	l_dst_root=$(planning_property_rows_with "$l_dst_root" compression lz4 \
		'inherited from dstpool/back')
	l_dst_root_set=$(
		printf '%s\n' "$l_dst_root"
		printf '%s\t%s\t%s\n' copies 2 local quota none default
	)
	l_dst_root=$(
		printf '%s\n' "$l_dst_root_set"
		printf '%s\t%s\t%s\n' checksum fletcher4 local com.x:path 'D:\old' local
	)
	l_dst_root_set=$(planning_property_rows_with "$l_dst_root_set" compression lz4 local)
	l_dst_root_set=$(
		printf '%s\n' "$l_dst_root_set"
		printf '%s\t%s\t%s\n' checksum sha256 local com.x:path 'D:\temp' local
	)
	l_inherited="inherited from $ZXFER_MOCKBIN_SOURCE_ROOT"
	l_src_child=$(planning_property_rows_with "$l_rows" compression lz4 "$l_inherited")
	l_src_child=$(
		printf '%s\n' "$l_src_child"
		printf '%s\t%s\t%s\n' checksum sha256 "$l_inherited" copies 2 'inherited from srcpool' \
			quota 1G received com.x:path 'D:\temp' "$l_inherited" volblocksize 8192 default
	)
	l_inherited="inherited from $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	l_dst_child=$(planning_property_rows_with "$l_rows" atime on local)
	l_dst_child=$(planning_property_rows_with "$l_dst_child" readonly off "$l_inherited")
	l_dst_child=$(
		printf '%s\n' "$l_dst_child"
		printf '%s\t%s\t%s\n' checksum fletcher4 "$l_inherited" copies 2 local \
			quota none default com.x:path 'D:\old' "$l_inherited"
	)
	planning_add_property_fixtures_for_rows "$l_src_root" "$l_src_child" \
		"$l_dst_root" "$l_dst_child"
	blackbox_properties_answer_live_reads "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$l_dst_root_set"

	(
		ZXFER_AWK_UNSUPPORTED_LIST=compression,checksum,quota
		ZXFER_AWK_REMOVE_LIST=atime,readonly
		export ZXFER_AWK_UNSUPPORTED_LIST ZXFER_AWK_REMOVE_LIST
		planning_run_zxfer "$STATE_DIR" -P -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	)
	assertEquals "the -P pass must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$?"
	l_expected="MUTATE set compression=lz4 checksum=sha256 com.x:path=D:\\temp $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	for l_child in child1 child2; do
		l_child="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/$l_child"
		l_expected="$l_expected
MUTATE set readonly=off atime=off quota=1G $l_child
MUTATE inherit compression $l_child
MUTATE inherit checksum $l_child
MUTATE inherit copies $l_child
MUTATE inherit com.x:path $l_child"
	done
	assertEquals "each rule plans its own set or inherit" \
		"$l_expected" "$(blackbox_properties_mutations)"
	assertEquals "a run without -v prints no property list" \
		0 "$(grep -c 'Property set list' "$CASE_DIR/zxfer.stdout")"
	planning_assert_no_send_receive
}

# The root's differing compression is set, which drops its cached view: the
# first child that needs its parent reads the root again, once, and the next
# child reuses that read. The canned zfs still answers gzip, so each child's
# inherit request is promoted to a local set. -V counts one tree read per
# side, and three live destination reads (child1, the parent, child2), one of
# them the parent's.
test_parent_is_read_again_once_after_its_set_and_children_it_does_not_match_are_set() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" parent_reread
	l_rows=$(planning_property_default_rows)
	planning_add_property_fixtures_for_rows "$l_rows" \
		"$(planning_property_rows_with "$l_rows" compression lz4 "inherited from $ZXFER_MOCKBIN_SOURCE_ROOT")" \
		"$(planning_property_rows_with "$l_rows" compression gzip local)" "$l_rows"

	planning_run_property_pass -V -P
	l_root=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
	assertEquals "the root is set and each child's inherit becomes a set" \
		"MUTATE set compression=lz4 $l_root
MUTATE set compression=lz4 $l_root/child1
MUTATE set compression=lz4 $l_root/child2" "$(blackbox_properties_mutations)"
	assertEquals "the root is read alone exactly once; zfs log: $(cat "$ZFS_LOG")" \
		1 "$(grep -cx "get -Hpo property,value,source all $l_root" "$ZFS_LOG")"
	l_set_at=$(planning_log_line_number "MUTATE set compression=lz4 $l_root")
	l_read_at=$(planning_log_line_number "get -Hpo property,value,source all $l_root")
	l_child_at=$(planning_log_line_number "MUTATE set compression=lz4 $l_root/child1")
	assertTrue "the read follows the root's set and precedes child1's change" \
		"[ '$l_set_at' -lt '$l_read_at' ] && [ '$l_read_at' -lt '$l_child_at' ]"
	for l_side_root in "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"; do
		assertEquals "one recursive read of $l_side_root" 1 \
			"$(grep -cx "get -r -t filesystem,volume -Hpo name,property,value,source all $l_side_root" "$ZFS_LOG")"
	done
	for l_counter in normalized_property_reads_source=1 normalized_property_reads_destination=4 \
		parent_destination_property_reads=1; do
		assertEquals "-V counts $l_counter" 1 \
			"$(grep -cx "zxfer profile: $l_counter" "$CASE_DIR/zxfer.stderr")"
	done
}

# Purpose: Seed STATE_DIR for the -o value cases: com.x:note=old on both
# roots and on neither side's children, with the argv recorder installed.
# Usage: blackbox_properties_add_override_fixture NAME
blackbox_properties_add_override_fixture() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" "$1"
	l_rows=$(planning_property_default_rows)
	l_root_rows=$(printf '%s\ncom.x:note\told\tlocal' "$l_rows")
	planning_add_property_fixtures_for_rows "$l_root_rows" "$l_rows" "$l_root_rows" "$l_rows"
	blackbox_properties_record_mutation_argv
}

# The -o text escapes one comma; the value keeps its "=", ";", "%", quotes,
# backslashes and command substitutions as one literal zfs set argument,
# locally and over -T, and runs none of them. Without -P the -o list is set
# on the root in -o order (mountpoint too, though zxfer never copies it) and
# every child inherits it, even com.x:note, which the children lack. With -P
# the root's list follows the source order, and a child that lacks an -o
# property gets nothing for it.
test_override_values_reach_zfs_set_whole_in_order_locally_and_over_T() {
	l_marker="$CASE_DIR/override-injected"
	l_value="a=b,c;% \$(: >$l_marker) \`: >$l_marker\` \"q\" \\n\\x"
	l_override="com.x:note=a=b\\,c;% \$(: >$l_marker) \`: >$l_marker\` \"q\" \\n\\x,compression=gzip,mountpoint=/srv/backup"
	l_root=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT

	blackbox_properties_add_override_fixture override_local
	planning_run_property_pass -o "$l_override"
	l_expected=$(
		printf '%s\n' 'ARGV 5' '[set]' "[com.x:note=$l_value]" '[compression=gzip]' \
			'[mountpoint=/srv/backup]' "[$l_root]"
		for l_child in child1 child2; do
			for l_property in com.x:note compression mountpoint; do
				printf '%s\n' 'ARGV 3' '[inherit]' "[$l_property]" "[$l_root/$l_child]"
			done
		done
	)
	assertEquals "-o: one set of the -o list in -o order, then one inherit per property" \
		"$l_expected" "$(cat "$ARGV_LOG")"

	blackbox_properties_add_override_fixture override_remote
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_property_pass -T localhost -P -o "$l_override"
	l_expected=$(
		printf '%s\n' 'ARGV 5' '[set]' '[mountpoint=/srv/backup]' '[compression=gzip]' \
			"[com.x:note=$l_value]" "[$l_root]"
		for l_child in child1 child2; do
			for l_property in mountpoint compression; do
				printf '%s\n' 'ARGV 3' '[inherit]' "[$l_property]" "[$l_root/$l_child]"
			done
		done
	)
	assertEquals "-T -P -o: the value crosses ssh whole; children without com.x:note get nothing for it" \
		"$l_expected" "$(cat "$ARGV_LOG")"
	assertFalse "no part of the value may run as a command" "[ -e '$l_marker' ]"
}

# -P -o compression=gzip,atime=off -k with both children missing. child1 is a
# volume: it is created with its size and its refreservation, the only
# creation-time item it has (its compression is inherited). child2 is created
# with its local and creation-time properties, but neither -o value: atime is
# inherited on the source, and the destination root already holds gzip. No
# type, volsize or creation-time property is probed, since every list holds
# them. Each created dataset is then seeded with -F, and after the receives
# the destination tree is read again for the reconcile while the source is
# not; -k records the created datasets' source lists.
test_missing_children_are_created_with_their_creation_properties_and_recorded() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" missing_children
	l_rows=$(planning_property_default_rows)
	l_src_inherited="inherited from $ZXFER_MOCKBIN_SOURCE_ROOT"
	l_dst_inherited="inherited from $ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	l_dst_root=$(planning_property_rows_with "$l_rows" compression gzip local)
	l_dst_root=$(planning_property_rows_with "$l_dst_root" atime on local)
	planning_add_property_fixtures_for_rows "$l_rows" \
		"$(planning_property_rows_with "$l_rows" atime off "$l_src_inherited")" \
		"$l_dst_root" "$l_rows"
	l_volume=$(printf '%s\t%s\t%s\n' type volume - volsize 1073741824 local \
		compression lz4 "$l_src_inherited" refreservation 1073741824 received \
		volblocksize 8192 -)
	blackbox_properties_use_rows src "$ZXFER_MOCKBIN_SOURCE_ROOT/child1" "$l_volume"
	l_dst_child1=$(printf '%s\t%s\t%s\n' type volume - volsize 1073741824 local \
		compression gzip "$l_dst_inherited" refreservation 1073741824 local \
		volblocksize 8192 -)
	l_dst_child2=$(planning_property_rows_with "$l_rows" compression gzip "$l_dst_inherited")
	l_dst_child2=$(planning_property_rows_with "$l_dst_child2" atime off "$l_dst_inherited")
	for l_child in child1 child2; do
		blackbox_properties_drop_destination_child "$l_child"
	done
	# What the reconcile after each seed reads back.
	blackbox_properties_answer_live_reads "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1" "$l_dst_child1"
	blackbox_properties_answer_live_reads "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2" "$l_dst_child2"
	l_backup_root="$CASE_DIR/backup"

	(
		ZXFER_BACKUP_DIR=$l_backup_root
		export ZXFER_BACKUP_DIR
		planning_run_zxfer "$STATE_DIR" -k -P -o compression=gzip,atime=off -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	)
	assertEquals "the -k -P -o pass must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$?"
	l_root=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
	assertEquals "the root's atime is set and each child is created once" \
		"MUTATE set atime=off $l_root
MUTATE create -V 1073741824 -o refreservation=1073741824 $l_root/child1
MUTATE create -o readonly=off -o casesensitivity=sensitive -o normalization=none -o utf8only=off $l_root/child2" \
		"$(blackbox_properties_mutations)"
	assertEquals "no type, volsize or creation-time property is probed; zfs log: $(cat "$ZFS_LOG")" \
		0 "$(grep -c -e '^get -Hpo value ' -e '^get .*casesensitivity' "$ZFS_LOG")"
	for l_child in child1 child2; do
		l_created_at=$(awk -v dataset="$l_root/$l_child" \
			'/^MUTATE create / && $NF == dataset { print NR; exit }' "$ZFS_LOG")
		assertTrue "$l_child is seeded with -F after its create" \
			"[ '$l_created_at' -lt '$(planning_log_line_number "receive -F $l_root/$l_child")' ]"
	done
	l_first_receive=$(planning_log_line_number "receive -F $l_root/child1")
	assertEquals "the reconcile reads the destination tree again" 2 \
		"$(grep -cx "get -r -t filesystem,volume -Hpo name,property,value,source all $ZXFER_MOCKBIN_DEST_ROOT" "$ZFS_LOG")"
	assertEquals "no source property is read after the first receive" "" \
		"$(awk -v from="$l_first_receive" -v root="$ZXFER_MOCKBIN_SOURCE_ROOT" \
			'NR > from && /^get / && index($0, " " root) { print }' "$ZFS_LOG")"
	l_backup_file=$(planning_backup_metadata_file "$l_backup_root" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT")
	assertEquals "-k records child1's source list" 1 "$(grep -cxF \
		"child1	type=volume=-,volsize=1073741824=local,compression=lz4=$l_src_inherited,refreservation=1073741824=received,volblocksize=8192=-" \
		"$l_backup_file")"
	assertEquals "-k records child2's source list" 1 "$(grep -cxF \
		"child2	type=filesystem=-,mountpoint=/mnt/data=local,compression=lz4=local,readonly=off=local,atime=off=$l_src_inherited,casesensitivity=sensitive=-,normalization=none=-,utf8only=off=-" \
		"$l_backup_file")"
}

# -o alone (no -P): the root is set and child2, missing, is created with the
# -o list and the creation-time properties only (readonly and atime are local
# but not in -o); the root's view, dropped by its set, is read again for it.
test_override_alone_creates_a_missing_child_with_the_list_and_creation_time_properties() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" override_create
	planning_add_property_transfer_fixtures
	blackbox_properties_drop_destination_child child2
	l_child2="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2"
	# What the reconcile after the seed reads back.
	blackbox_properties_answer_live_reads "$l_child2" "$(planning_property_rows_with \
		"$(planning_property_default_rows)" compression gzip "inherited from $ZXFER_MOCKBIN_DEST_MAPPED_ROOT")"

	planning_run_property_pass -o compression=gzip
	assertEquals "root set, child1 inherit, child2 create" \
		"MUTATE set compression=gzip $ZXFER_MOCKBIN_DEST_MAPPED_ROOT
MUTATE inherit compression $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1
MUTATE create -o compression=gzip -o casesensitivity=sensitive -o normalization=none -o utf8only=off $l_child2" \
		"$(blackbox_properties_mutations)"
}

# child2 is created by someone else after discovery: the recursive listing
# lacks it, but its live probe finds it, so its properties are diffed and
# set instead of created again.
test_destination_created_after_discovery_is_diffed_not_created() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" created_after_discovery
	l_rows=$(planning_property_default_rows)
	planning_add_property_fixtures_for_rows "$l_rows" "$l_rows" "$l_rows" "$l_rows"
	l_child2="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2"
	blackbox_properties_drop_destination_child child2
	printf '%s\t96K\t1.0G\t24K\t/%s\n' "$l_child2" "$l_child2" >"$STATE_DIR/child2_exists.list" ||
		fail "Unable to write the child2 listing."
	blackbox_properties_prepend_rules "list -H $l_child2	child2_exists.list	0"
	# The reconcile after its seed reads the set value back.
	blackbox_properties_answer_live_reads "$l_child2" \
		"$(planning_property_rows_with "$l_rows" compression gzip local)" once

	planning_run_property_pass -P
	assertEquals "the found child is set, never created" \
		"MUTATE set compression=lz4 $l_child2" "$(blackbox_properties_mutations)"
	assertTrue "the live probe precedes the set" \
		"[ '$(planning_log_line_number "list -H $l_child2")' -lt '$(planning_log_line_number "MUTATE set compression=lz4 $l_child2")' ]"
}

# The destination root and its parent are missing. -P -o
# casesensitivity=insensitive creates the parent with -p, then the root with
# its whole -P list (checksum's default value too) and the creation-time -o
# value; each child is created with its creation list minus the -o value its
# new parent now supplies. The destination rows are the state after the
# creates.
# shellcheck disable=SC2089,SC2090  # the quotes are part of the zfs message
test_missing_destination_root_is_created_with_its_whole_list_after_its_parent() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" missing_root
	l_rows=$(planning_property_default_rows)
	l_src_rows=$(printf '%s\nchecksum\ton\tdefault' "$l_rows")
	l_dst_root=$(planning_property_rows_with "$l_rows" casesensitivity insensitive -)
	l_dst_child=$(printf '%s\nchecksum\ton\tinherited from %s' "$l_dst_root" \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT")
	l_dst_root=$(printf '%s\nchecksum\ton\tlocal' "$l_dst_root")
	planning_add_property_fixtures_for_rows "$l_src_rows" "$l_src_rows" \
		"$l_dst_root" "$l_dst_child"
	l_pool=${ZXFER_MOCKBIN_DEST_ROOT%%/*}
	printf '%s\n' "$l_pool" >"$STATE_DIR/dst_pool.list" ||
		fail "Unable to write the pool listing."
	blackbox_properties_prepend_rules "list -H -o name $l_pool	dst_pool.list	0"
	planning_force_manifest_failure \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" 1
	for l_missing in "$ZXFER_MOCKBIN_DEST_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2"; do
		l_missing_file="missing_$(printf '%s' "$l_missing" | tr '/' '_').list"
		printf "cannot open '%s': dataset does not exist\n" "$l_missing" \
			>"$STATE_DIR/$l_missing_file" || fail "Unable to write the missing-dataset line."
		blackbox_properties_prepend_rules "list -H $l_missing	$l_missing_file	1"
	done
	mkdir -p "$CASE_DIR/fail_calls" || fail "Unable to create the fault counter."

	(
		MOCK_FAIL_TOOL=zfs
		MOCK_FAIL_CALL=1
		MOCK_FAIL_DIR="$CASE_DIR/fail_calls"
		MOCK_FAIL_MATCH="list -t filesystem,volume -Hr -o name $ZXFER_MOCKBIN_DEST_ROOT"
		MOCK_FAIL_STDERR="cannot open '$ZXFER_MOCKBIN_DEST_ROOT': dataset does not exist"
		export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH MOCK_FAIL_STDERR
		planning_run_zxfer "$STATE_DIR" -P -o casesensitivity=insensitive -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	)
	assertEquals "the pass must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$?"
	l_root=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
	l_child_create="-o compression=lz4 -o readonly=off -o atime=off -o normalization=none -o utf8only=off"
	assertEquals "the parent, the root and each child are created once" \
		"MUTATE create -p $ZXFER_MOCKBIN_DEST_ROOT
MUTATE create -o compression=lz4 -o readonly=off -o atime=off -o casesensitivity=insensitive -o normalization=none -o utf8only=off -o checksum=on $l_root
MUTATE create $l_child_create $l_root/child1
MUTATE create $l_child_create $l_root/child2" "$(blackbox_properties_mutations)"
}

# Purpose: Run ./zxfer -R over STATE_DIR with the first zfs call matching
# GLOB failing with STDERR and status 1 (an empty STDERR fails it silently).
# Usage: blackbox_properties_run_failing GLOB STDERR [zxfer-arg...]; sets
# BLACKBOX_PROPERTIES_STATUS to zxfer's status.
blackbox_properties_run_failing() {
	l_failing_match=$1
	l_failing_stderr=$2
	shift 2
	rm -rf "$CASE_DIR/fail_calls"
	mkdir "$CASE_DIR/fail_calls" || fail "Unable to create the fault counter."
	BLACKBOX_PROPERTIES_STATUS=0
	(
		MOCK_FAIL_TOOL=zfs
		MOCK_FAIL_CALL=1
		MOCK_FAIL_DIR="$CASE_DIR/fail_calls"
		MOCK_FAIL_MATCH=$l_failing_match
		MOCK_FAIL_STDERR=$l_failing_stderr
		export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH MOCK_FAIL_STDERR
		planning_run_zxfer "$STATE_DIR" "$@" -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	) </dev/null || BLACKBOX_PROPERTIES_STATUS=$?
}

# Purpose: Fail unless the last run exited EXPECTED with one runtime failure
# report naming STAGE and exactly MESSAGE, and started no change after the
# injected failure (or at all, when none was injected).
# Usage: blackbox_properties_assert_stopped LABEL EXPECTED STATUS STAGE MESSAGE
blackbox_properties_assert_stopped() {
	assertEquals "$1: exit status; stderr: $(cat "$CASE_DIR/zxfer.stderr")" "$2" "$3"
	for l_stopped_line in "exit_status: $2" "failure_class: runtime" "failure_stage: $4" \
		"message: $5"; do
		grep -Fqx -e "$l_stopped_line" "$CASE_DIR/zxfer.stderr" ||
			fail "$1: missing report line: $l_stopped_line
stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	done
	assertEquals "$1: exactly one failure report" \
		1 "$(grep -c '^zxfer: failure report begin$' "$CASE_DIR/zxfer.stderr")"
	assertEquals "$1: no change starts after the failure; zfs log: $(cat "$ZFS_LOG")" "" \
		"$(awk '/^FAIL / { failed = 1 } /^MUTATE / && (failed || !injected) { print }' \
			injected="$(grep -c '^FAIL ' "$ZFS_LOG")" "$ZFS_LOG")"
}

# Purpose: Seed STATE_DIR in CASE_DIR/NAME with the default rows, but no
# casesensitivity, normalization or utf8only on the source (some OpenZFS
# builds leave them out of `zfs get all`), and answer each source dataset's
# one comma-list get of the three with FIXTURE and STATUS.
# Usage: blackbox_properties_add_backfill_fixture NAME FIXTURE STATUS
blackbox_properties_add_backfill_fixture() {
	: >"$ZFS_LOG"
	planning_clone_state "$FIXTURE_DIR/noop" "$1"
	l_rows=$(planning_property_default_rows)
	l_src_rows=$(printf '%s\n' "$l_rows" |
		grep -v -e '^casesensitivity	' -e '^normalization	' -e '^utf8only	')
	planning_add_property_fixtures_for_rows "$l_src_rows" "$l_src_rows" "$l_rows" "$l_rows"
	printf '%s\t%s\t%s\n' casesensitivity sensitive - normalization none - utf8only off - \
		>"$STATE_DIR/creation_rows.list" || fail "Unable to write the creation rows."
	blackbox_properties_prepend_rules \
		"get -Hpo property,value,source casesensitivity,normalization,utf8only $ZXFER_MOCKBIN_SOURCE_ROOT*	$2	$3"
}

# The source lists lack the creation-time properties. Each source dataset
# reads the three with one comma-list get. When that get fails, each is read
# alone after "--", and one that does not apply is left out. A value that
# differs on the existing destination is refused before any change.
test_missing_creation_time_properties_are_read_back_and_enforced() {
	planning_setup_env
	blackbox_properties_add_backfill_fixture backfill_batch creation_rows.list 0
	planning_run_property_pass -V -P
	planning_assert_no_mutations
	assertEquals "one comma-list get per source dataset; zfs log: $(cat "$ZFS_LOG")" 3 \
		"$(grep -c "^get -Hpo property,value,source casesensitivity,normalization,utf8only $ZXFER_MOCKBIN_SOURCE_ROOT" "$ZFS_LOG")"
	assertEquals "no property is read alone" 0 "$(grep -c -- ' -- ' "$ZFS_LOG")"
	assertEquals "-V counts the three gets" 1 \
		"$(grep -cx 'zxfer profile: required_property_backfill_gets=3' "$CASE_DIR/zxfer.stderr")"

	blackbox_properties_add_backfill_fixture backfill_alone - 1
	for l_property in casesensitivity normalization utf8only; do
		grep "^$l_property	" "$STATE_DIR/creation_rows.list" >"$STATE_DIR/alone_$l_property.list" ||
			fail "Unable to write the $l_property row."
		blackbox_properties_prepend_rules \
			"get -Hpo property,value,source -- $l_property $ZXFER_MOCKBIN_SOURCE_ROOT*	alone_$l_property.list	0"
	done
	blackbox_properties_run_failing \
		"get -Hpo property,value,source -- utf8only $ZXFER_MOCKBIN_SOURCE_ROOT" \
		"bad property list: property 'utf8only' does not apply to datasets of this type" -P
	assertEquals "an inapplicable property is left out; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$BLACKBOX_PROPERTIES_STATUS"
	planning_assert_no_mutations
	assertEquals "after a failed comma-list get, each property is read alone after --" 9 \
		"$(grep -c '^get -Hpo property,value,source -- [a-z0-9]* ' "$ZFS_LOG")"

	blackbox_properties_add_backfill_fixture backfill_refused creation_rows.list 0
	sed 's/^casesensitivity	sensitive/casesensitivity	insensitive/' \
		"$STATE_DIR/creation_rows.list" >"$STATE_DIR/insensitive_rows.list" ||
		fail "Unable to write the insensitive rows."
	blackbox_properties_prepend_rules \
		"get -Hpo property,value,source casesensitivity,normalization,utf8only $ZXFER_MOCKBIN_SOURCE_ROOT	insensitive_rows.list	0"
	planning_run_zxfer "$STATE_DIR" -P -R "$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	blackbox_properties_assert_stopped "creation-time mismatch" 1 "$?" "property transfer" \
		'The property "casesensitivity" may only be set\nat filesystem creation time. To modify this property\nyou will need to first destroy target filesystem.'
	planning_assert_no_send_receive
}

# -U compares the property names of the source and of the destination
# filesystem, probes only the names the destination lacks (never a user
# property), and skips overlay, which the destination rejects, and volmode,
# which does not apply to a destination of the same type. -v warns once for
# each list a skipped property was in: overlay (local) is in the apply and
# creation lists, volmode (default) in the apply list only.
test_skip_unsupported_probes_only_missing_names_and_warns_about_each_skip() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" unsupported_decisions
	l_dst_rows=$(planning_property_default_rows)
	l_src_rows=$(
		printf '%s\n' "$l_dst_rows"
		printf '%s\t%s\t%s\n' overlay on local volmode default default com.x:note x local
	)
	planning_add_property_fixtures_for_rows "$l_src_rows" "$l_src_rows" \
		"$l_dst_rows" "$l_dst_rows"
	printf "property 'volmode' does not apply to datasets of this type\n" \
		>"$STATE_DIR/volmode_not_applicable.list" || fail "Unable to write the volmode answer."
	blackbox_properties_prepend_rules \
		"get -Hpo property,value,source volmode $ZXFER_MOCKBIN_DEST_MAPPED_ROOT*	volmode_not_applicable.list	1"
	planning_add_unsupported_property_fixtures "$l_src_rows" "$l_dst_rows"

	planning_run_property_pass -U -v -P
	l_root=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
	assertEquals "only the user property is set" \
		"MUTATE set com.x:note=x $l_root
MUTATE set com.x:note=x $l_root/child1
MUTATE set com.x:note=x $l_root/child2" "$(blackbox_properties_mutations)"
	assertEquals "only the two names the destination lacks are probed" \
		"get -Hpo property,value,source overlay $l_root
get -Hpo property,value,source volmode $l_root" \
		"$(grep "^get -Hpo property,value,source [^ ]* $ZXFER_MOCKBIN_DEST_ROOT" "$ZFS_LOG" |
			grep -v ' all ')"
	assertEquals "overlay is reported for both lists of each dataset" 6 \
		"$(grep -cx 'Destination does not support property overlay=on' "$CASE_DIR/zxfer.stderr")"
	assertEquals "volmode is reported for the apply list of each dataset" 3 \
		"$(grep -cx 'Destination does not support property volmode=default' "$CASE_DIR/zxfer.stderr")"
}

# child1 is a volume on both sides, and every dataset has a snapshot to send
# (-U scans the types of those). The destination volume lacks dedup, so -U
# probes dedup there and skips it for volumes only: both filesystems get
# dedup=on, the volume does not.
test_skip_unsupported_keeps_volume_and_filesystem_skips_apart() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" unsupported_volume
	l_rows=$(planning_property_default_rows)
	l_src_fs=$(printf '%s\ndedup\ton\tlocal' "$l_rows")
	l_dst_fs=$(printf '%s\ndedup\toff\tlocal' "$l_rows")
	planning_add_property_fixtures_for_rows "$l_src_fs" "$l_src_fs" "$l_dst_fs" "$l_dst_fs"
	l_volume=$(printf '%s\t%s\t%s\n' type volume - volsize 1073741824 local compression lz4 local)
	blackbox_properties_use_rows src "$ZXFER_MOCKBIN_SOURCE_ROOT/child1" \
		"$(printf '%s\ndedup\ton\tlocal' "$l_volume")"
	blackbox_properties_use_rows dst "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1" "$l_volume"
	printf '%s\tfilesystem\n%s\tvolume\n%s\tfilesystem\n' "$ZXFER_MOCKBIN_SOURCE_ROOT" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT/child1" "$ZXFER_MOCKBIN_SOURCE_ROOT/child2" \
		>"$STATE_DIR/src_types.list" || fail "Unable to write the source types."
	printf 'volume\n' >"$STATE_DIR/type_volume.list" || fail "Unable to write the volume type."
	for l_names in src_fs:"$l_src_fs" dst_fs:"$l_dst_fs" src_volume:"$(printf '%s\ndedup' "$l_volume")" \
		dst_volume:"$l_volume"; do
		printf '%s\n' "${l_names#*:}" | cut -f1 >"$STATE_DIR/names_${l_names%%:*}.list" ||
			fail "Unable to write the ${l_names%%:*} property names."
	done
	printf "bad property list: invalid property 'dedup'\n" >"$STATE_DIR/dedup_unknown.list" ||
		fail "Unable to write the dedup answer."
	l_src=$ZXFER_MOCKBIN_SOURCE_ROOT
	l_dst=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
	blackbox_properties_prepend_rules \
		"get -Hpo name,value type $l_src*	src_types.list	0" \
		"get -Hpo property all $l_src	names_src_fs.list	0" \
		"get -Hpo property all $l_src/child1	names_src_volume.list	0" \
		"get -Hpo value type $l_dst/child1	type_volume.list	0" \
		"get -Hpo property all $l_dst	names_dst_fs.list	0" \
		"get -Hpo property all $l_dst/child1	names_dst_volume.list	0" \
		"get -Hpo property,value,source dedup $l_dst/child1	dedup_unknown.list	1"

	planning_run_property_pass -U -P
	assertEquals "dedup is set on the filesystems only" \
		"MUTATE set dedup=on $l_dst
MUTATE set dedup=on $l_dst/child2" "$(blackbox_properties_mutations)"
	assertEquals "dedup is probed on the destination volume only" \
		"get -Hpo property,value,source dedup $l_dst/child1" \
		"$(grep '^get -Hpo property,value,source [^ ]* ' "$ZFS_LOG" | grep -v ' all ')"
}

# zxfer reads the destination's platform from uname (on the local host
# here). On FreeBSD, aclmode is one of the properties zxfer never sets; on
# another platform it is set like any differing local property.
test_freebsd_destination_never_sets_its_read_only_properties() {
	for l_os in FreeBSD Linux; do
		planning_setup_env
		: >"$ZFS_LOG"
		planning_clone_state "$FIXTURE_DIR/noop" "platform_$l_os"
		l_rows=$(planning_property_default_rows)
		l_dst_rows=$(planning_property_rows_with "$l_rows" compression gzip local)
		planning_add_property_fixtures_for_rows "$(printf '%s\naclmode\tpassthrough\tlocal' "$l_rows")" \
			"$(printf '%s\naclmode\tpassthrough\tlocal' "$l_rows")" \
			"$(printf '%s\naclmode\tdiscard\tlocal' "$l_dst_rows")" \
			"$(printf '%s\naclmode\tdiscard\tlocal' "$l_dst_rows")"
		if ! printf '#!/bin/sh\nprintf "%%s\\n" %s\n' "$l_os" >"$MOCKBIN_DIR/uname" ||
			! chmod +x "$MOCKBIN_DIR/uname"; then
			fail "Unable to write the uname stand-in."
		fi

		planning_run_property_pass -P
		l_set="compression=lz4"
		[ "$l_os" = FreeBSD ] || l_set="$l_set aclmode=passthrough"
		assertEquals "$l_os: the sets" "MUTATE set $l_set $ZXFER_MOCKBIN_DEST_MAPPED_ROOT
MUTATE set $l_set $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1
MUTATE set $l_set $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2" "$(blackbox_properties_mutations)"
	done
}

# Purpose: Print how many control bytes other than LF and TAB a file holds.
# Usage: blackbox_properties_count_control_bytes FILE
blackbox_properties_count_control_bytes() {
	LC_ALL=C tr -dc '\001-\010\013-\037\177' <"$1" | wc -c | tr -d ' '
}

# -v prints, for each changed dataset, the decoded set and inherit lists and
# each command before it runs: the local zfs command, or over -T the ssh
# command line. A value holding control bytes is shown escaped in the set and
# inherit lists and in the -V list dumps, while zfs gets it raw.
test_verbose_shows_decoded_lists_and_each_command() {
	l_root=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
	for l_mode in local remote; do
		planning_setup_env
		planning_clone_state "$FIXTURE_DIR/noop" "verbose_$l_mode"
		l_rows=$(planning_property_default_rows)
		planning_add_property_fixtures_for_rows "$l_rows" \
			"$(planning_property_rows_with "$l_rows" compression lz4 "inherited from $ZXFER_MOCKBIN_SOURCE_ROOT")" \
			"$(planning_property_rows_with "$l_rows" atime on local)" "$l_rows"
		if [ "$l_mode" = local ]; then
			planning_run_property_pass -v -P
			for l_line in "1 Setting properties/sources on destination filesystem \"$l_root\"." \
				"1 Property set list: atime=off" "1 '$MOCKBIN_DIR/zfs' 'set' 'atime=off' '$l_root'" \
				"1 Setting properties/sources on destination filesystem \"$l_root/child1\"." \
				"2 Property inherit list: compression=lz4" \
				"1 '$MOCKBIN_DIR/zfs' 'inherit' 'compression' '$l_root/child1'" \
				"1 '$MOCKBIN_DIR/zfs' 'inherit' 'compression' '$l_root/child2'"; do
				assertEquals "-v prints: ${l_line#* }; stdout: $(cat "$CASE_DIR/zxfer.stdout")" \
					"${l_line%% *}" "$(grep -cxF -e "${l_line#* }" "$CASE_DIR/zxfer.stdout")"
			done
		else
			planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
				fail "Unable to write socket-aware mock ssh."
			PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
				planning_run_property_pass -T localhost -v -P
			assertEquals "-T -v prints the ssh command line of the set; stdout: $(cat "$CASE_DIR/zxfer.stdout")" 1 \
				"$(grep -F "'localhost'" "$CASE_DIR/zxfer.stdout" |
					grep -cF "'\\''set'\\'' '\\''atime=off'\\'' '\\''$l_root'\\''")"
		fi
	done

	# Each child sets com.x:tag and inherits com.x:note, both holding l_value.
	l_value=$(printf 'ok\033[2J\033]0;owned\007 lit=\\033[31m \\c cut\r')
	l_shown='ok\x1B[2J\x1B]0;owned\x07 lit=\\033[31m \\c cut\r'
	l_shown_encoded='ok\x1B[2J\x1B]0%3Bowned\x07 lit%3D\\033[31m \\c cut%0D'
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" verbose_hostile
	l_rows=$(planning_property_default_rows)
	l_note_rows=$(printf '%s\ncom.x:note\t%s\tlocal' "$l_rows" "$l_value")
	planning_add_property_fixtures_for_rows "$l_note_rows" \
		"$(printf '%s\ncom.x:note\t%s\tinherited from %s\ncom.x:tag\t%s\tlocal' "$l_rows" \
			"$l_value" "$ZXFER_MOCKBIN_SOURCE_ROOT" "$l_value")" \
		"$l_note_rows" "$(printf '%s\ncom.x:tag\told\tlocal' "$l_note_rows")"
	blackbox_properties_record_mutation_argv
	planning_run_property_pass -v -V -P
	for l_line in "Property set list: com.x:tag=$l_shown" \
		"Property inherit list: com.x:note=$l_shown"; do
		assertEquals "-v shows once per child: $l_line" 2 \
			"$(grep -cxF -e "$l_line" "$CASE_DIR/zxfer.stdout")"
	done
	for l_line in "zxfer_transfer_properties adjusted child_set: com.x:tag=$l_shown_encoded" \
		"zxfer_transfer_properties adjusted inherit: com.x:note=$l_shown_encoded"; do
		assertEquals "-V dumps once per child: $l_line" 2 \
			"$(grep -cxF -e "$l_line" "$CASE_DIR/zxfer.stderr")"
	done
	assertEquals "stdout holds no control byte but LF and TAB" \
		0 "$(blackbox_properties_count_control_bytes "$CASE_DIR/zxfer.stdout")"
	assertEquals "stderr holds no control byte but LF and TAB" \
		0 "$(blackbox_properties_count_control_bytes "$CASE_DIR/zxfer.stderr")"
	l_expected=$(
		for l_child in child1 child2; do
			printf '%s\n' 'ARGV 3' '[set]' "[com.x:tag=$l_value]" "[$l_root/$l_child]" \
				'ARGV 3' '[inherit]' '[com.x:note]' "[$l_root/$l_child]"
		done
	)
	assertEquals "zfs set gets the raw value, zfs inherit the name alone" \
		"$l_expected" "$(cat "$ARGV_LOG")"
}

# A pass reads each dataset alone (both value views, then the name list)
# when it has no tree to read: -N reads one dataset per side. When the
# source tree read fails, each source dataset is read alone and the tree is
# never tried again; the destination tree is still read once.
test_property_reads_fall_back_to_one_dataset_at_a_time() {
	l_src=$ZXFER_MOCKBIN_SOURCE_ROOT
	l_dst=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" read_nonrecursive
	planning_add_property_transfer_fixtures
	planning_run_zxfer "$STATE_DIR" -V -P -N "$l_src" "$ZXFER_MOCKBIN_DEST_ROOT"
	assertEquals "-N -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$?"
	l_reads=$(grep '^get ' "$ZFS_LOG")
	assertEquals "-N reads the dataset of each side alone, name list last" \
		"get -Hpo property,value,source all $l_src
get -Ho property,value,source all $l_src
get -Ho property all $l_src
get -Hpo property,value,source all $l_dst
get -Ho property,value,source all $l_dst
get -Ho property all $l_dst" "$l_reads"
	for l_side in source destination; do
		assertEquals "-V counts one $l_side read" 1 \
			"$(grep -cx "zxfer profile: normalized_property_reads_$l_side=1" "$CASE_DIR/zxfer.stderr")"
	done

	: >"$ZFS_LOG"
	planning_clone_state "$FIXTURE_DIR/noop" read_tree_failure
	planning_add_property_transfer_fixtures
	planning_force_manifest_failure \
		"get -r -t filesystem,volume -Hpo name,property,value,source all $l_src" 1
	planning_run_property_pass -P
	planning_assert_no_mutations
	assertEquals "the failed source tree read is tried once" "get -r -t filesystem,volume -Hpo name,property,value,source all $l_src" \
		"$(grep "^get -r .* all $l_src\$" "$ZFS_LOG")"
	for l_suffix in "" /child1 /child2; do
		assertEquals "$l_src$l_suffix is read alone once" 1 \
			"$(grep -cx "get -Hpo property,value,source all $l_src$l_suffix" "$ZFS_LOG")"
	done
	assertEquals "the destination tree is read once" 1 \
		"$(grep -cx "get -r -t filesystem,volume -Hpo name,property,value,source all $ZXFER_MOCKBIN_DEST_ROOT" "$ZFS_LOG")"
}

# Each -Y pass starts from empty property tables: the source root, which the
# tree read cannot publish (its com.x:note spans two lines), is read alone
# again in every pass. The canned zfs never receives, so every pass sends and
# -Y repeats up to its limit.
test_each_yield_pass_reads_properties_again() {
	blackbox_properties_add_multiline_root_fixture yield_passes 'a@LF@b' incremental
	planning_run_property_pass -Y -P
	l_passes=$(grep -cx "get -r -t filesystem,volume -Hpo name,property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT" "$ZFS_LOG")
	assertTrue "-Y must run more than one pass; zfs log: $(cat "$ZFS_LOG")" "[ '$l_passes' -gt 1 ]"
	assertEquals "the root is read alone once per pass" "$l_passes" \
		"$(grep -cx "get -Hpo property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT" "$ZFS_LOG")"
	assertEquals "the destination tree is read once per pass" "$l_passes" \
		"$(grep -cx "get -r -t filesystem,volume -Hpo name,property,value,source all $ZXFER_MOCKBIN_DEST_ROOT" "$ZFS_LOG")"
}

# Purpose: Clone the noop state into CASE_DIR/NAME with the given row sets
# (the default rows where one is empty) and start an empty zfs log.
# Usage: blackbox_properties_add_rows_fixture NAME [SRC_ROOT SRC_CHILD
# DST_ROOT DST_CHILD]
blackbox_properties_add_rows_fixture() {
	: >"$ZFS_LOG"
	planning_clone_state "$FIXTURE_DIR/noop" "$1"
	l_rows=$(planning_property_default_rows)
	planning_add_property_fixtures_for_rows "${2:-$l_rows}" "${3:-$l_rows}" \
		"${4:-$l_rows}" "${5:-$l_rows}"
}

# Each failed read stops the pass before any change, with the zfs exit status
# and a report that names the dataset, the step and the zfs diagnostic: the
# source and destination reads (after a failed tree read), a destination
# listing that repeats a name, a lone re-read, a creation-time property read,
# and a source whose type or zvol size is unusable.
test_property_read_failures_stop_with_the_dataset_and_the_zfs_diagnostic() {
	planning_setup_env
	l_src=$ZXFER_MOCKBIN_SOURCE_ROOT
	l_dst=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
	l_rows=$(planning_property_default_rows)
	l_denied="cannot open '$l_src': permission denied"

	blackbox_properties_add_rows_fixture read_source
	planning_force_manifest_failure \
		"get -r -t filesystem,volume -Hpo name,property,value,source all $l_src" 1
	blackbox_properties_run_failing "get -Hpo property,value,source all $l_src" "$l_denied" -P
	blackbox_properties_assert_stopped "source read" 1 "$BLACKBOX_PROPERTIES_STATUS" \
		"property transfer" "$l_denied"

	blackbox_properties_add_rows_fixture read_destination
	planning_force_manifest_failure \
		"get -r -t filesystem,volume -Hpo name,property,value,source all $ZXFER_MOCKBIN_DEST_ROOT" 1
	blackbox_properties_run_failing "get -Hpo property,value,source all $l_dst" \
		"cannot open '$l_dst': permission denied" -P
	blackbox_properties_assert_stopped "destination read" 1 "$BLACKBOX_PROPERTIES_STATUS" \
		"property transfer" \
		"Failed to retrieve destination properties for [$l_dst]: cannot open '$l_dst': permission denied"

	blackbox_properties_add_rows_fixture repeated_name
	blackbox_properties_use_rows dst "$l_dst" "$(printf '%s\ncompression\tgzip\tlocal' "$l_rows")"
	planning_run_zxfer "$STATE_DIR" -P -R "$l_src" "$ZXFER_MOCKBIN_DEST_ROOT"
	blackbox_properties_assert_stopped "repeated destination name" 1 "$?" "property transfer" \
		"Failed to retrieve destination properties for [$l_dst]: Failed to parse the properties of dataset [$l_dst]: the zfs get property list is malformed, repeats a name, or does not match the values."

	blackbox_properties_add_multiline_root_fixture lone_read 'a@LF@b'
	: >"$ZFS_LOG"
	blackbox_properties_run_failing "get -Ho property,value,source -- com.x:note $l_src" \
		"$l_denied" -P
	blackbox_properties_assert_stopped "lone re-read" 1 "$BLACKBOX_PROPERTIES_STATUS" \
		"property transfer" "Failed to read property [com.x:note] of dataset [$l_src]: $l_denied"

	blackbox_properties_add_backfill_fixture creation_read - 1
	blackbox_properties_run_failing "get -Hpo property,value,source -- casesensitivity $l_src" \
		"permission denied" -P
	blackbox_properties_assert_stopped "creation-time property read" 1 \
		"$BLACKBOX_PROPERTIES_STATUS" "property transfer" \
		"Failed to retrieve required creation-time property [casesensitivity] for dataset [$l_src]: permission denied"

	blackbox_properties_add_rows_fixture snapshot_type
	blackbox_properties_use_rows src "$l_src" "$(planning_property_rows_with "$l_rows" type snapshot -)"
	planning_run_zxfer "$STATE_DIR" -P -R "$l_src" "$ZXFER_MOCKBIN_DEST_ROOT"
	blackbox_properties_assert_stopped "source type" 1 "$?" "property transfer" \
		"Invalid source dataset type for [$l_src]: snapshot"

	blackbox_properties_add_rows_fixture empty_volsize
	blackbox_properties_use_rows src "$l_src" \
		"$(printf '%s\t%s\t%s\n' type volume - volsize - - compression lz4 local)"
	planning_run_zxfer "$STATE_DIR" -P -R "$l_src" "$ZXFER_MOCKBIN_DEST_ROOT"
	blackbox_properties_assert_stopped "zvol size" 1 "$?" "property transfer" \
		"Failed to retrieve source zvol size for [$l_src]: empty volsize"
}

# A failed set, inherit or create stops the pass at that command, with the
# step's message; so do a failed existence probe of a destination the
# listing lacks, and a failed read of the parent a create or an inherit
# needs (the root's view, dropped by its set, is read again).
test_property_change_failures_stop_at_the_failing_command() {
	planning_setup_env
	l_src=$ZXFER_MOCKBIN_SOURCE_ROOT
	l_dst=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
	l_rows=$(planning_property_default_rows)
	l_child_inherits=$(planning_property_rows_with "$l_rows" compression lz4 "inherited from $l_src")
	l_root_differs=$(planning_property_rows_with "$l_rows" compression gzip local)

	blackbox_properties_add_rows_fixture set_failure "" "" "$l_root_differs"
	blackbox_properties_run_failing "set *" "cannot set property for '$l_dst': permission denied" -P
	blackbox_properties_assert_stopped "set" 1 "$BLACKBOX_PROPERTIES_STATUS" \
		"property transfer" "Error when setting properties on destination filesystem."

	blackbox_properties_add_rows_fixture inherit_failure "" "$l_child_inherits"
	blackbox_properties_run_failing "inherit *" \
		"cannot inherit compression for '$l_dst/child1': permission denied" -P
	blackbox_properties_assert_stopped "inherit" 1 "$BLACKBOX_PROPERTIES_STATUS" \
		"property transfer" "Error when inheriting properties on destination filesystem."

	blackbox_properties_add_rows_fixture create_failure
	blackbox_properties_drop_destination_child child2
	blackbox_properties_run_failing "create *" "cannot create '$l_dst/child2': permission denied" -P
	blackbox_properties_assert_stopped "create" 1 "$BLACKBOX_PROPERTIES_STATUS" \
		"property transfer" "Error when creating destination filesystem."

	blackbox_properties_add_rows_fixture probe_failure
	blackbox_properties_drop_destination_child child2
	blackbox_properties_run_failing "list -H $l_dst/child2" \
		"cannot open '$l_dst/child2': permission denied" -P
	blackbox_properties_assert_stopped "existence probe" 1 "$BLACKBOX_PROPERTIES_STATUS" \
		"property transfer" \
		"Failed to determine whether destination dataset [$l_dst/child2] exists: cannot open '$l_dst/child2': permission denied"

	blackbox_properties_add_rows_fixture create_parent_read
	blackbox_properties_drop_destination_child child1
	blackbox_properties_run_failing "get -Hpo property,value,source all $l_dst" \
		"cannot open '$l_dst': permission denied" -o compression=gzip
	blackbox_properties_assert_stopped "parent read for a create" 1 \
		"$BLACKBOX_PROPERTIES_STATUS" "property transfer" \
		"Failed to retrieve parent destination properties for [$l_dst]: cannot open '$l_dst': permission denied"

	blackbox_properties_add_rows_fixture inherit_parent_read "" "$l_child_inherits" "$l_root_differs"
	blackbox_properties_run_failing "get -Hpo property,value,source all $l_dst" \
		"cannot open '$l_dst': permission denied" -P
	blackbox_properties_assert_stopped "parent read for an inherit" 1 \
		"$BLACKBOX_PROPERTIES_STATUS" "property transfer" \
		"Failed to reconcile inherited child properties for destination [$l_dst/child1]."
}

# Every failed -U probe stops the run in discovery, before any change: the
# source types, the source and destination property names, the destination
# probe dataset's type, and a support probe that fails with a message or
# silently.
test_skip_unsupported_scan_failures_stop_before_any_change() {
	planning_setup_env
	l_src=$ZXFER_MOCKBIN_SOURCE_ROOT
	l_dst=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
	l_scan_dst=$(planning_property_default_rows)
	l_scan_src=$(printf '%s\noverlay\ton\tlocal' "$l_scan_dst")
	l_probe="Failed to probe destination support for property [overlay] on [$l_dst]"
	l_scan_rows=0
	while IFS='|' read -r l_scan_name l_scan_match l_scan_stderr l_scan_message; do
		l_scan_rows=$((l_scan_rows + 1))
		blackbox_properties_add_rows_fixture "$l_scan_name" "$l_scan_src" "$l_scan_src" \
			"$l_scan_dst" "$l_scan_dst"
		planning_add_unsupported_property_fixtures "$l_scan_src" "$l_scan_dst"
		blackbox_properties_run_failing "$l_scan_match" "$l_scan_stderr" -U -P
		blackbox_properties_assert_stopped "$l_scan_name" 1 "$BLACKBOX_PROPERTIES_STATUS" \
			"snapshot discovery" "$l_scan_message"
	done <<EOF
source_types|get -Hpo name,value type *|permission denied|Failed to retrieve source dataset types for unsupported-property scan: permission denied
source_names|get -Hpo property all $l_src|permission denied|Failed to retrieve source property list for dataset [$l_src]: permission denied
probe_type|get -Hpo value type $l_dst|permission denied|Failed to determine the destination property-support probe dataset type for [$l_dst]: permission denied
destination_names|get -Hpo property all $l_dst|permission denied|Failed to retrieve destination property list for dataset [$l_dst]: permission denied
support_probe|get -Hpo property,value,source overlay $l_dst|permission denied|$l_probe: permission denied
silent_probe|get -Hpo property,value,source overlay $l_dst||$l_probe: probe exited nonzero without stdout/stderr
EOF
	assertEquals "every failure row ran" 6 "$l_scan_rows"
}

# -e restores each dataset from its own row of the backup file: a dataset
# whose row is missing, or repeated, is a usage error before any change.
test_restore_row_missing_or_repeated_for_a_dataset_is_a_usage_error() {
	planning_setup_backup_env restore_rows
	planning_run_backup_zxfer -k -P
	assertEquals "-k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$?"
	cp "$PRIMARY_FILE" "$CASE_DIR/backup.saved" || fail "Unable to keep the backup file."
	l_pair="filesystem $ZXFER_MOCKBIN_SOURCE_ROOT/child1 and destination $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1"

	grep -v '^child1	' "$CASE_DIR/backup.saved" >"$CASE_DIR/backup.edit" ||
		fail "Unable to drop the child1 row."
	cat "$CASE_DIR/backup.edit" >"$PRIMARY_FILE" || fail "Unable to rewrite the backup file."
	: >"$ZFS_LOG"
	planning_run_backup_zxfer -e
	assertEquals "a missing row must exit 2" 2 "$?"
	blackbox_properties_assert_usage_report "property transfer" \
		"Can't find the properties for the $l_pair"
	planning_assert_no_mutations

	{
		cat "$CASE_DIR/backup.saved"
		grep '^child1	' "$CASE_DIR/backup.saved"
	} >"$CASE_DIR/backup.edit" || fail "Unable to repeat the child1 row."
	cat "$CASE_DIR/backup.edit" >"$PRIMARY_FILE" || fail "Unable to rewrite the backup file."
	: >"$ZFS_LOG"
	planning_run_backup_zxfer -e
	assertEquals "a repeated row must exit 2" 2 "$?"
	blackbox_properties_assert_usage_report "property transfer" \
		"Multiple restored property entries matched $l_pair"
	planning_assert_no_mutations
}

. "$SHUNIT2_BIN"
