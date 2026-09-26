#!/bin/sh
#
# Black-box pins for -v/-V operator output: the real ./zxfer launcher runs the
# property pass over the canned zfs from tests/helpers/blackbox.sh, with a
# source user property whose value holds terminal control sequences.
#
# Pins:
#   test_verbose_output_escapes_hostile_property_values
#   → under -v -V, locally and over -T, with the launcher run by /bin/sh and
#     by dash when it is installed: stdout and stderr hold no control byte but
#     LF and TAB, the property lines show the value escaped, its LF forges no
#     line, and zfs set still gets the raw value as one argument.
#
# shellcheck disable=SC1090,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

# shellcheck source=tests/helpers/blackbox.sh
. "$TESTS_DIR/helpers/blackbox.sh"

# The source value, spelling LF as @LF@ for the fixture rows: ESC sequences
# that clear the screen and set the terminal title, BEL, the literal text
# \033 and \c that echo expands, CR, and an LF followed by a forged line.
VERBOSE_HOSTILE_VALUE=$(printf 'ok\033[2J\033]0;owned\007 lit=\\033[31m \\c cut\r@LF@Property set list: forged=1')

# Purpose: Seed STATE_DIR with default property rows plus com.x:note on the
# source root holding VERBOSE_HOSTILE_VALUE, answer its lone reads (an LF
# makes zxfer read it alone), and record every create, set and inherit argv.
# Usage: verbose_add_hostile_root_fixture NAME
verbose_add_hostile_root_fixture() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" "$1"
	l_rows=$(planning_property_default_rows)
	l_root_rows=$(printf '%s\ncom.x:note\t%s\tlocal' "$l_rows" "$VERBOSE_HOSTILE_VALUE")
	planning_add_property_fixtures_for_rows "$l_root_rows" "$l_rows" "$l_rows" "$l_rows"
	printf 'com.x:note\t%s\tlocal\n' "$VERBOSE_HOSTILE_VALUE" >"$STATE_DIR/src_note.list"
	printf '%s\tsrc_note.list\t0\n' \
		"get -Hpo property,value,source -- com.x:note $ZXFER_MOCKBIN_SOURCE_ROOT" \
		"get -Ho property,value,source -- com.x:note $ZXFER_MOCKBIN_SOURCE_ROOT" \
		>>"$STATE_DIR/manifest" || fail "Unable to append the lone-read rules."
	for l_list in src_props_tree.list src_props_root.list src_note.list; do
		if ! awk '{ gsub(/@LF@/, "\n"); print }' "$STATE_DIR/$l_list" >"$STATE_DIR/$l_list.new" ||
			! mv "$STATE_DIR/$l_list.new" "$STATE_DIR/$l_list"; then
			fail "Unable to expand the LF in $l_list."
		fi
	done

	ARGV_LOG="$CASE_DIR/argv.log"
	: >"$ARGV_LOG"
	mv "$MOCKBIN_DIR/zfs" "$MOCKBIN_DIR/zfs.canned" ||
		fail "Unable to stage the canned zfs behind the argv recorder."
	cat >"$MOCKBIN_DIR/zfs" <<EOF
#!/bin/sh
case "\${1:-}" in
create | set | inherit) printf '[%s]\n' "\$@" >>"$ARGV_LOG" ;;
esac
exec "$MOCKBIN_DIR/zfs.canned" "\$@"
EOF
	chmod +x "$MOCKBIN_DIR/zfs"
}

# Purpose: Print how many control bytes other than LF and TAB (C0 or DEL) a
# file holds. TAB appears in snapshot records (name TAB guid).
# Usage: verbose_count_control_bytes FILE
verbose_count_control_bytes() {
	LC_ALL=C tr -dc '\001-\010\013-\037\177' <"$1" | wc -c | tr -d ' '
}

# Purpose: Print how many lines of FILE equal LINE exactly.
# Usage: verbose_count_exact_lines FILE LINE
verbose_count_exact_lines() {
	LC_ALL=C grep -c -F -x -e "$2" "$1"
}

test_verbose_output_escapes_hostile_property_values() {
	l_shown='com.x:note=ok\x1B[2J\x1B]0;owned\x07 lit=\\033[31m \\c cut\r\nProperty set list: forged=1'
	l_shown_encoded='com.x:note=ok\x1B[2J\x1B]0%3Bowned\x07 lit%3D\\033[31m \\c cut%0D%0AProperty set list: forged%3D1'
	l_raw=${VERBOSE_HOSTILE_VALUE%%@LF@*}$ZXFER_LF${VERBOSE_HOSTILE_VALUE#*@LF@}
	l_expected_argv=$(printf '[set]\n[com.x:note=%s]\n[%s]' "$l_raw" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT")

	l_dash=$(command -v dash 2>/dev/null) || l_dash=""
	for l_launcher in sh dash; do
		if [ "$l_launcher" = dash ]; then
			[ -n "$l_dash" ] || continue
			ZXFER_MOCKBIN_ZXFER_BIN="$CASE_DIR/zxfer_under_dash"
			printf '#!/bin/sh\nexec "%s" "%s" "$@"\n' "$l_dash" "$ZXFER_ROOT/zxfer" \
				>"$ZXFER_MOCKBIN_ZXFER_BIN"
			chmod +x "$ZXFER_MOCKBIN_ZXFER_BIN"
		fi
		for l_mode in local remote; do
			l_case="$l_launcher $l_mode"
			verbose_add_hostile_root_fixture "${l_launcher}_$l_mode"
			if [ "$l_mode" = local ]; then
				planning_run_property_pass -v -V -P
			else
				planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
					fail "Unable to write socket-aware mock ssh."
				PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
					planning_run_property_pass -T localhost -v -V -P
			fi

			assertEquals "$l_case: stdout holds no control byte but LF and TAB" \
				0 "$(verbose_count_control_bytes "$CASE_DIR/zxfer.stdout")"
			assertEquals "$l_case: stderr holds no control byte but LF and TAB" \
				0 "$(verbose_count_control_bytes "$CASE_DIR/zxfer.stderr")"
			assertEquals "$l_case: -v shows the set list escaped on one line" \
				1 "$(verbose_count_exact_lines "$CASE_DIR/zxfer.stdout" "Property set list: $l_shown")"
			assertEquals "$l_case: the value's LF forges no line" \
				0 "$(grep -c '^Property set list: forged' "$CASE_DIR/zxfer.stdout")"
			assertEquals "$l_case: -V shows the encoded init_set escaped" \
				1 "$(verbose_count_exact_lines "$CASE_DIR/zxfer.stderr" \
					"zxfer_transfer_properties init_set: $l_shown_encoded")"
			assertEquals "$l_case: zfs set gets the raw value as one argument" \
				"$l_expected_argv" "$(cat "$ARGV_LOG")"
		done
	done
	unset ZXFER_MOCKBIN_ZXFER_BIN
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
