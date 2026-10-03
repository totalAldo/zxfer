#!/bin/sh
#
# shunit2 tests for src/zxfer_property_state.sh: the untrusted zfs get
# parser, serialization, the in-memory property tables, and the reads the
# black-box suites cannot fault or shape: hostile lone re-reads, racing
# recursive reads, row-store failures and backfill capture shapes. Read
# order, routing, caching, backfill and read failures are pinned black-box
# in tests/test_contract_properties.sh.
#
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"
# shellcheck source=tests/helpers/property_fixtures.sh
. "$TESTS_DIR/helpers/property_fixtures.sh"

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_property_state"
	zxfer_test_property_fixture_one_time_setup
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	zxfer_test_property_fixture_setup
}

################################################################################
# PARSING AND ENCODING
################################################################################

# Purpose: Stage printf %b captures and run the one-dataset parser on them.
# Usage: zxfer_property_test_parse_merge SKELETON MACHINE [HUMAN]; the human
# view defaults to the machine view. Prints the parser output.
zxfer_property_test_parse_merge() {
	printf '%b' "$1" >"$TEST_TMPDIR/parse.skeleton"
	printf '%b' "$2" >"$TEST_TMPDIR/parse.machine"
	printf '%b' "${3-$2}" >"$TEST_TMPDIR/parse.human"
	zxfer_parse_property_views merge "$TEST_TMPDIR/parse.skeleton" \
		"$TEST_TMPDIR/parse.machine" "$TEST_TMPDIR/parse.human"
}

# Purpose: Run the one-dataset parser on printf %b captures and assert its
# status and output.
# Usage: zxfer_property_test_assert_merge LABEL SKELETON MACHINE HUMAN STATUS
# OUTPUT; HUMAN "=" reuses MACHINE.
zxfer_property_test_assert_merge() {
	l_merge_human=$4
	[ "$l_merge_human" != "=" ] || l_merge_human=$3
	l_merge_status=0
	l_merge_output=$(zxfer_property_test_parse_merge "$2" "$3" "$l_merge_human") ||
		l_merge_status=$?
	assertEquals "$1: status" "$5" "$l_merge_status"
	assertEquals "$1" "$6" "$l_merge_output"
}

# The parser publishes a record only where the name list pins it to one line
# in an unbroken run from the first line; the first record it cannot place,
# and every later one, is left to a lone re-read (the second output line).
test_parse_property_views_publishes_only_records_the_name_list_pins_to_one_line() {
	zxfer_property_test_assert_merge "one-line records keep their TABs and are encoded" \
		'user:note\nuser:tab\ncompression\n' \
		'user:note\tvalue,=; mix\tlocal\nuser:tab\ta\tb\tinherited from tank\ncompression\tlz4\tlocal\n' \
		= 0 "user:note=value%2C%3D%3B mix=local,user:tab=a%09b=inherited from tank,compression=lz4=local"
	zxfer_property_test_assert_merge "the run ends at a multi-line record; it and every later one are re-read" \
		'compression\nuser:note\nuser:tail\n' \
		'compression\tlz4\tlocal\nuser:note\tline1\nline2\tlocal\nuser:tail\tt\tlocal\n' \
		= 0 "compression=lz4=local,user:note,user:tail
user:note,user:tail"
	zxfer_property_test_assert_merge "a value shaped like a second record is re-read, never cut at the fake one" \
		'compression\nuser:record\n' \
		'compression\tlz4\tlocal\nuser:record\tx\tlocal\nuser:fake\ty\tlocal\n' \
		= 0 "compression=lz4=local,user:record
user:record"
	zxfer_property_test_assert_merge "a continuation shaped like the next real record leaves both ambiguous" \
		'compression\nuser:a\nuser:b\n' \
		'compression\tlz4\tlocal\nuser:a\tx\tlocal\nuser:b\toff\tlocal\nuser:b\ton\tlocal\n' \
		= 0 "compression=lz4=local,user:a,user:b
user:a,user:b"
	zxfer_property_test_assert_merge "a continuation that repeats its own record's name is ambiguous too" \
		'compression\nuser:z\n' 'compression\tlz4\tlocal\nuser:z\tv\tlocal\nuser:z\tw\tlocal\n' \
		= 0 "compression,user:z
compression,user:z"
	zxfer_property_test_assert_merge "a human none replaces the machine value, in name-list order" \
		'quota\ncompression\n' 'quota\t1073741824\tlocal\ncompression\tlz4\tlocal\n' \
		'quota\tnone\tlocal\ncompression\tlz4\tlocal\n' 0 "quota=none=local,compression=lz4=local"
	zxfer_property_test_assert_merge "a property one view lacks cannot be placed, so every record is re-read" \
		'compression\natime\n' 'compression\tlz4\tlocal\natime\toff\tlocal\n' \
		'compression\tlz4\tlocal\nrecordsize\t128K\tdefault\n' 0 "compression,atime
compression,atime"
	zxfer_property_test_assert_merge "a key that heads two lines ends the run before the record ahead of it" \
		'compression\natime\nuser:x\n' \
		'compression\tlz4\tlocal\natime\toff\tlocal\nuser:x\tv\natime\ton\tlocal\n' \
		= 0 "compression,atime,user:x
compression,atime,user:x"
	zxfer_property_test_assert_merge "a listed record the views lack ends the run at line 1, so a copy in u:x decides nothing" \
		'mounted\norigin\nquota\nreadonly\nu:x\n' \
		'mounted\tyes\t-\nquota\tnone\tdefault\nreadonly\toff\tdefault\nu:x\tv\norigin\tpool/a@s\t-\nquota\tnone\tdefault\nreadonly\ton\tlocal\nu:x\tz\tlocal\n' \
		= 0 "mounted,origin,quota,readonly,u:x
mounted,origin,quota,readonly,u:x"
	zxfer_property_test_assert_merge "backslashes stay literal" \
		'user:path\n' 'user:path\tC:\\temp\\new\tlocal\n' = 0 'user:path=C:\temp\new=local'
	for l_skeleton in 'compression\ncompression\n' 'bad\trow\n' 'not a name\n' ''; do
		zxfer_property_test_assert_merge "skeleton [$l_skeleton] fails the parse and prints nothing" \
			"$l_skeleton" 'compression\tlz4\tlocal\n' = 1 ""
	done
	zxfer_property_test_assert_merge "empty views of an empty skeleton parse to nothing" '' '' = 0 ""
}

# Pins the documented Low window (KNOWN_ISSUES.md): a user property created
# between the value views and the name list is listed while the views lack
# it, so the lines of the value printed just before it (user:car) pass for
# its record. Each view below is exactly what zfs prints for user:car's
# value; hardening the parser turns these around.
test_parse_property_views_residual_created_record_takes_text_from_the_value_before_it() {
	result=$(zxfer_property_test_parse_merge 'type\nuser:car\nuser:k\nzz:tail\n' \
		'type\tfilesystem\t-\nuser:car\tx\tlocal\nuser:k\tFAKE\tlocal\nzz:tail\tt\tlocal\n')
	assertEquals "user:car='x<TAB>local<LF>user:k<TAB>FAKE' is cut and forges user:k" \
		"type=filesystem=-,user:car=x=local,user:k=FAKE=local,zz:tail=t=local" "$result"

	result=$(zxfer_property_test_parse_merge 'type\nuser:car\nuser:k\nzz:tail\n' \
		'type\tfilesystem\t-\nuser:car\tx\tlocal\nuser:k\tFAKE\tlocal\nmore\tlocal\nzz:tail\tt\tlocal\n')
	assertEquals "user:k is re-read alone, but user:car stays cut" \
		"type=filesystem=-,user:car=x=local,user:k,zz:tail
user:k,zz:tail" "$result"

	result=$(zxfer_property_test_parse_merge 'type\nuser:car\nuser:k\n' \
		'type\tfilesystem\t-\nuser:car\tx\treceived\nuser:k\tFAKE\tlocal\n')
	assertEquals "user:car, set locally, takes the source received from its own text" \
		"type=filesystem=-,user:car=x=received,user:k=FAKE=local" "$result"

	result=$(zxfer_property_test_parse_merge 'type\nuser:car\nuser:k1\nuser:k2\nzz:tail\n' \
		'type\tfilesystem\t-\nuser:car\tx\tdefault\nuser:k1\tA\tinherited from p/x\nuser:k2\tB\tlocal\nzz:tail\tt\tlocal\n')
	assertEquals "one value forges a run of created properties and their sources" \
		"type=filesystem=-,user:car=x=default,user:k1=A=inherited from p/x,user:k2=B=local,zz:tail=t=local" "$result"

	# In a recursive read, a dataset created between the calls (p/new, not
	# wanted) cuts the value printed before it the same way.
	printf 'p/a\np/b\n' >"$TEST_TMPDIR/parse.wanted"
	printf 'p/a\ttype\np/a\tuser:car\np/new\ttype\np/b\ttype\n' >"$TEST_TMPDIR/parse.skeleton"
	printf 'p/a\ttype\tfilesystem\t-\np/a\tuser:car\tx\tlocal\np/new\ttype\tvolume\tlocal\np/b\ttype\tfilesystem\t-\n' \
		>"$TEST_TMPDIR/parse.machine"
	zxfer_prepare_property_read_files
	index=$(zxfer_parse_property_views store "$TEST_TMPDIR/parse.wanted" \
		"$TEST_TMPDIR/parse.skeleton" "$TEST_TMPDIR/parse.machine" "$TEST_TMPDIR/parse.machine")
	assertEquals "The recursive parser must store the accepted rows." 0 "$?"
	result=$(
		g_zxfer_source_property_table=$ZXFER_LF$index$ZXFER_LF
		zxfer_property_test_table_dump source
	)
	assertEquals "p/a is published with user:car cut" \
		"p/a	type=filesystem=-,user:car=x=local
p/b	type=filesystem=-" "$result"
}

test_encode_property_value_matches_the_awk_encoder_and_round_trips() {
	l_awk_encoder="$ZXFER_PROPERTY_AWK_LIB"'
BEGIN { printf "%s", encode_value(ENVIRON["ZXFER_TEST_VALUE"]) }'
	for l_value in "" "plain" "%" "%25" "a,b=c;d" "$(printf 'tab\there\rcr\nlf')" \
		"$(printf 'soh\001 %%0A \\n')"; do
		l_awk_value=$(
			ZXFER_TEST_VALUE=$l_value "${g_cmd_awk:-awk}" "$l_awk_encoder"
			printf x
		)
		zxfer_encode_property_value "$l_value"
		assertEquals "encode of [$l_value]" "${l_awk_value%x}" "$g_zxfer_encoded_property_value"
		zxfer_decode_property_value "$g_zxfer_encoded_property_value"
		assertEquals "round trip of [$l_value]" "$l_value" "$g_zxfer_decoded_property_value"
	done
}

test_split_property_record_cuts_the_value_at_the_last_tab() {
	zxfer_split_property_record user:x "$(printf 'user:x\ta\tlocal\nuser:y\tb\tlocal')"
	assertEquals 0 "$?"
	assertEquals "$(printf 'a\tlocal\nuser:y\tb')" "$g_zxfer_property_record_value"
	assertEquals "local" "$g_zxfer_property_record_source"
	for l_record in "$(printf 'user:y\ta\tlocal')" "$(printf 'user:x\ta')" \
		"$(printf 'user:x\ta\tsideways')" "$(printf 'user:x\ta\tinherited from ')"; do
		l_status=0
		zxfer_split_property_record user:x "$l_record" || l_status=$?
		assertEquals "[$l_record] is not a user:x record with a known source" 1 "$l_status"
	done
	# A value may start and end with a line feed.
	zxfer_split_property_record user:multi "$(printf 'user:multi\t\nmid\n\tinherited from tank')"
	l_expected=$(printf '\nmid\nx')
	assertEquals "${l_expected%x}" "$g_zxfer_property_record_value"
	assertEquals "inherited from tank" "$g_zxfer_property_record_source"
}

test_decode_property_value_undoes_each_code_in_awk_order_without_awk() {
	zxfer_decode_property_value "x%2Cy%3Dz%3B%25%09t%0Dr%0Al"
	assertEquals "$(printf 'x,y=z;%%\tt\rr\nl')" "$g_zxfer_decoded_property_value"
	zxfer_decode_property_value "%250A%2%2C%%3D"
	assertEquals "%25 decodes last, so an encoded percent never forms a new code." \
		"%0A%2,%=" "$g_zxfer_decoded_property_value"
	l_awk_decoder="$ZXFER_PROPERTY_AWK_LIB"'
BEGIN { printf "%s", decode_value(ENVIRON["ZXFER_TEST_VALUE"]) }'
	for l_value in "" "plain" "%" "%%0A" "%0%0A" "%2%2C" "%3%3B" "%252C%2C" \
		"a%0D%0Ab" "%250D%0D" "%3D%3D=%3d" "tail%"; do
		l_awk_value=$(
			ZXFER_TEST_VALUE=$l_value "${g_cmd_awk:-awk}" "$l_awk_decoder"
			printf x
		)
		zxfer_decode_property_value "$l_value"
		assertEquals "decode of [$l_value] matches awk" "${l_awk_value%x}" "$g_zxfer_decoded_property_value"
	done
	(
		g_cmd_awk="$TEST_TMPDIR/missing-awk"
		zxfer_decode_property_value "$(printf 'a\001-o\001b%%2C')"
		printf '%s' "$g_zxfer_decoded_property_value"
	) >"$TEST_TMPDIR/decode_plain.out"
	assertEquals "Control bytes pass through, and no awk runs." \
		"$(printf 'a\001-o\001b,')" "$(cat "$TEST_TMPDIR/decode_plain.out")"
}

test_decode_serialized_property_list_for_display_decodes_values() {
	zxfer_decode_serialized_property_list_for_display "user:note=a%2Cb%3Dc=local,,quota=1G"
	assertEquals "user:note=a,b=c=local,quota=1G" "$g_zxfer_property_display_list_result"
}

test_property_list_value_returns_the_first_exact_match() {
	zxfer_property_list_value "type=volume=-,volsize=1G=local,volsize=2G=local" volsize
	assertEquals 0 "$?"
	assertEquals "1G=local" "$g_zxfer_property_list_value_result"
	l_lookup_status=0
	zxfer_property_list_value "volsize=1G=local" vol || l_lookup_status=$?
	assertEquals "A name prefix must not match." 1 "$l_lookup_status"
	assertEquals "A miss clears the result." "" "$g_zxfer_property_list_value_result"
}

################################################################################
# IN-MEMORY TABLES
################################################################################

# An index line names one whole dataset: a lookup never matches a prefix, a
# glob, another table's row or a key spanning lines, and returns the payload
# byte for byte whatever the name holds.
test_property_tables_match_whole_literal_dataset_names_only() {
	l_payload='user:note=  literal\path%09tab%0Aline%25%2C%3D  =local'
	for l_dataset in '../unsafe path:/child' 'tank/src/[literal]*\name' 'tank/a b' 'tank/a'; do
		zxfer_property_test_table_add source "$l_dataset" "$l_payload"
	done
	for l_dataset in '../unsafe path:/child' 'tank/src/[literal]*\name' 'tank/a b' 'tank/a'; do
		l_lookup_status=0
		zxfer_property_table_find_dataset source "$l_dataset" || l_lookup_status=$?
		assertEquals "[$l_dataset] is found at any position" 0 "$l_lookup_status"
		assertEquals "[$l_dataset] keeps its payload byte for byte" \
			"$l_payload" "$g_zxfer_property_table_lookup_result"
	done
	for l_key in tank 'a b' 'tank/a b/c' 'tank/sr' '[literal]' '*' ''; do
		assertFalse "[$l_key] names no row." "zxfer_property_table_find_dataset source \"\$l_key\""
	done
	assertFalse "An unknown side must miss." "zxfer_property_table_find_dataset other tank/a"
	assertFalse "The destination table must not answer source lookups." \
		"zxfer_property_table_find_dataset destination tank/a"

	l_index=$(printf '\np3\ttank/a b\np2\ttank/a\np1\t[x]*\n.')
	l_index=${l_index%.}
	zxfer_find_property_row "$l_index" "tank/a"
	assertEquals "p2" "$g_zxfer_property_row_result"
	zxfer_find_property_row "$l_index" "[x]*"
	assertEquals "Glob bytes in a key are literal." "p1" "$g_zxfer_property_row_result"
	for l_key in "[x]" "*" ""; do
		assertFalse "[$l_key] names no line." "zxfer_find_property_row \"\$l_index\" \"\$l_key\""
	done
	# Without the TAB and LF guard this key would match across two lines
	# and answer with tank/a b's row.
	l_key=$(printf 'tank/a b\np2\ttank/a')
	assertFalse "A key spanning lines never matches." \
		"zxfer_find_property_row \"\$l_index\" \"\$l_key\""
}

# The newest row of a dataset wins, and a lookup reads that row alone: an
# empty, cut-short (no final LF) or unreadable newest row is a silent miss
# that hides every older row.
test_property_table_lookup_reads_the_newest_row_alone_and_misses_a_bad_one() {
	zxfer_property_test_table_add source tank/src "stale=1=local"
	zxfer_property_test_table_add source tank/src "fresh=1=local"
	zxfer_property_table_find_dataset source tank/src
	assertEquals "The newest row wins." "fresh=1=local" "$g_zxfer_property_table_lookup_result"
	zxfer_property_test_table_add source tank/src ""
	l_lookup_status=0
	zxfer_property_table_find_dataset source tank/src || l_lookup_status=$?
	assertEquals "An empty newest row must not expose an older duplicate." 1 "$l_lookup_status"
	assertEquals "A miss must clear the previous lookup." "" "$g_zxfer_property_table_lookup_result"

	zxfer_property_test_table_add source pool/x "compression=lz4=local"
	l_x_row=$g_zxfer_property_row_result
	zxfer_property_test_table_add source pool/x/a "compression=lz4=inherited from pool/x"
	zxfer_property_test_table_add source pool/x/b "atime=off=local"
	l_b_row=$g_zxfer_property_row_result
	# Replace pool/x's row with a directory: a lookup that scanned every row
	# would trip over it.
	rm -f "${g_zxfer_property_row_dir:?}/${l_x_row:?}"
	mkdir "$g_zxfer_property_row_dir/$l_x_row"
	zxfer_property_table_find_dataset source pool/x/a
	assertEquals "compression=lz4=inherited from pool/x" "$g_zxfer_property_table_lookup_result"
	printf 'atime=of' >"$g_zxfer_property_row_dir/$l_b_row"
	l_lookup_status=0
	l_stderr=$(zxfer_property_table_find_dataset source pool/x/b 2>&1) || l_lookup_status=$?
	assertEquals "A row without its final LF is a miss." "1:" "$l_lookup_status:$l_stderr"
	l_lookup_status=0
	l_stderr=$(zxfer_property_table_find_dataset source pool/x 2>&1) || l_lookup_status=$?
	assertEquals "An unreadable row is a miss." "1:" "$l_lookup_status:$l_stderr"
}

# A change drops the destination rows it may have changed: set, inherit and
# create (the default, subtree) drop the dataset and its descendants, a
# receive (exact) the dataset alone, with a tombstone that needs no process;
# siblings, source rows and the prefetch state stay. A failed strip empties
# the destination table, which only forces live reads, and no dataset resets
# it and re-arms its prefetch.
test_invalidate_destination_property_mutation_cache_drops_what_a_change_may_have_changed() {
	zxfer_property_test_table_add destination "backup/dst" "compression=lz4=local"
	zxfer_property_test_table_add destination "backup/dst/child" "compression=gzip=inherited"
	zxfer_property_test_table_add destination "backup/dst2" "atime=off=local"
	zxfer_property_test_table_add source "backup/dst" "compression=lz4=local"
	g_zxfer_destination_property_tree_prefetch_state=1
	zxfer_invalidate_destination_property_mutation_cache "backup/dst"
	assertFalse "The changed dataset goes." "zxfer_property_table_find_dataset destination backup/dst"
	assertFalse "Its descendants go: their inherited values may have changed." \
		"zxfer_property_table_find_dataset destination backup/dst/child"
	assertTrue "A sibling stays." "zxfer_property_table_find_dataset destination backup/dst2"
	assertTrue "Source rows are never dropped." "zxfer_property_table_find_dataset source backup/dst"
	assertEquals "The prefetched tree stays warm." 1 "$g_zxfer_destination_property_tree_prefetch_state"

	zxfer_reset_property_iteration_caches
	zxfer_property_test_table_add destination "backup/dst" "compression=lz4=local"
	zxfer_property_test_table_add destination "backup/dst/child" "compression=lz4=inherited"
	zxfer_property_test_table_add destination "../unsafe path:/child" "user:note=line1%0Aline2=local"
	zxfer_invalidate_destination_property_mutation_cache "backup/dst" exact
	zxfer_invalidate_destination_property_mutation_cache "../unsafe path:/child" exact
	assertFalse "A receive drops its dataset." "zxfer_property_table_find_dataset destination backup/dst"
	assertFalse "A hostile name is dropped too." \
		"zxfer_property_table_find_dataset destination '../unsafe path:/child'"
	assertTrue "A receive leaves descendant rows warm." \
		"zxfer_property_table_find_dataset destination backup/dst/child"

	zxfer_reset_property_iteration_caches
	zxfer_property_test_table_add destination "backup/dst" "compression=lz4=local"
	zxfer_property_test_table_add destination "backup/dst/child" "compression=lz4=inherited"
	(
		# A receive, or a change of a dataset without descendant rows, needs
		# no process: a tombstone hides the rows.
		g_cmd_awk="$TEST_TMPDIR/missing-awk"
		zxfer_invalidate_destination_property_mutation_cache "backup/dst" exact
		zxfer_invalidate_destination_property_mutation_cache "backup/dst/child"
		zxfer_invalidate_destination_property_mutation_cache "backup/none" exact
		zxfer_property_test_table_dump destination
		# The live read after the tombstone is the newest row again.
		zxfer_property_test_table_add destination "backup/dst" "compression=gzip=local"
		zxfer_property_table_find_dataset destination backup/dst
		printf 'after=%s\n' "$g_zxfer_property_table_lookup_result"
		# A strip that cannot run empties the table.
		zxfer_invalidate_destination_property_mutation_cache "backup/dst" 2>/dev/null
		printf 'failed strip=<%s>\n' "${g_zxfer_destination_property_table:-}"
	) >"$TEST_TMPDIR/tombstone.out" 2>&1
	assertEquals "backup/dst/child	-
backup/dst	-
backup/dst/child	compression=lz4=inherited
backup/dst	compression=lz4=local
after=compression=gzip=local
failed strip=<>" "$(cat "$TEST_TMPDIR/tombstone.out")"

	zxfer_property_test_table_add source "tank/src" "compression=lz4=local"
	g_zxfer_destination_property_tree_prefetch_state=1
	zxfer_invalidate_destination_property_mutation_cache ""
	assertEquals "No dataset resets the destination table." "" "${g_zxfer_destination_property_table:-}"
	assertTrue "Source rows survive a destination-wide reset." \
		"zxfer_property_table_find_dataset source tank/src"
	assertEquals "A destination-wide reset re-arms the destination prefetch." \
		0 "$g_zxfer_destination_property_tree_prefetch_state"
}

################################################################################
# LIVE READS
################################################################################

# Purpose: Answer `zfs get` for zxfer_run_zfs_cmd_for_role from a property
# model, printing values raw as zfs does, and log each call to ROLE_LOG.
# ZXFER_TEST_PROPERTY_ROWS holds one "dataset<TAB>property<TAB>value<TAB>
# source" row per record, in zfs order, with TAB and LF in values spelled \t
# and \n; ZXFER_TEST_PROPERTY_HUMAN_ROWS, when set, answers the -Ho views, and
# ZXFER_TEST_PROPERTY_LATER_ROWS, when set, answers the name lists and lone
# reads, which zxfer makes after the value views. Like zfs's getopt, it takes
# an operand that starts with "-" as an option unless -- comes first.
# Usage: zxfer_property_test_model_zfs ROLE get [-r -t TYPES] -H[p]o COLUMNS
# [--] PROPERTY|all DATASET
zxfer_property_test_model_zfs() {
	printf '%s\n' "$*" >>"${ROLE_LOG:-/dev/null}"
	shift 2
	l_model_recursive=0
	if [ "$1" = -r ]; then
		l_model_recursive=1
		shift 3
	fi
	case $3 in
	--) set -- "$1" "$2" "$4" "$5" ;;
	-?*)
		l_model_option=${3#-}
		printf "invalid option '%s'\n" "${l_model_option%"${l_model_option#?}"}" >&2
		return 2
		;;
	esac
	l_model_rows=$ZXFER_TEST_PROPERTY_ROWS
	[ "$1" != -Ho ] || l_model_rows=${ZXFER_TEST_PROPERTY_HUMAN_ROWS:-$l_model_rows}
	case "$2 $3" in
	*"property,value,source all") ;;
	*) l_model_rows=${ZXFER_TEST_PROPERTY_LATER_ROWS:-$l_model_rows} ;;
	esac
	l_model_found=0
	while IFS=$ZXFER_TAB read -r l_model_dataset l_model_property l_model_value l_model_source; do
		case $l_model_dataset in
		"$4") ;;
		"$4"/*) [ "$l_model_recursive" -eq 1 ] || continue ;;
		*) continue ;;
		esac
		[ "$3" = all ] || [ "$3" = "$l_model_property" ] || continue
		l_model_found=1
		case $2 in
		property) printf '%s\n' "$l_model_property" ;;
		name,property) printf '%s\t%s\n' "$l_model_dataset" "$l_model_property" ;;
		property,value,source)
			printf '%s\t%b\t%s\n' "$l_model_property" "$l_model_value" "$l_model_source"
			;;
		name,property,value,source)
			printf '%s\t%s\t%b\t%s\n' "$l_model_dataset" "$l_model_property" \
				"$l_model_value" "$l_model_source"
			;;
		esac
	done <<EOF
$l_model_rows
EOF
	[ "$l_model_found" -eq 1 ] && return 0
	printf "cannot open '%s': dataset does not exist\n" "$4" >&2
	return 1
}

test_load_normalized_dataset_properties_never_takes_a_listed_property_from_a_copy_in_a_value() {
	ROLE_LOG="$TEST_TMPDIR/role_listed_missing.log"
	: >"$ROLE_LOG"
	(
		# origin appears after the value views and before the name list;
		# u:x's value copies the listing's tail with readonly=on.
		l_copy='v\norigin\tpool/a@s\t-\nquota\tnone\tdefault\nreadonly\ton\tlocal\nu:x\tz'
		ZXFER_TEST_PROPERTY_ROWS=$(printf '%s\t%s\t%s\t%s\n' \
			tank/src mounted yes - \
			tank/src quota none default \
			tank/src readonly off default \
			tank/src u:x "$l_copy" local)
		ZXFER_TEST_PROPERTY_LATER_ROWS=$(printf '%s\t%s\t%s\t%s\n' \
			tank/src mounted yes - \
			tank/src origin 'pool/a@s' - \
			tank/src quota none default \
			tank/src readonly off default \
			tank/src u:x "$l_copy" local)
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_load_normalized_dataset_properties "tank/src" source
		printf 'status=%s list=%s\n' "$?" "$g_zxfer_normalized_dataset_properties"
	) >"$TEST_TMPDIR/normalized_listed_missing.out"
	assertEquals "Every property is read alone, so readonly keeps its real value." \
		"status=0 list=mounted=yes=-,origin=pool/a@s=-,quota=none=default,readonly=off=default,u:x=v%0Aorigin%09pool/a@s%09-%0Aquota%09none%09default%0Areadonly%09on%09local%0Au:x%09z=local" \
		"$(cat "$TEST_TMPDIR/normalized_listed_missing.out")"
	assertEquals "Five properties, two lone reads each." 10 "$(grep -vc ' all ' "$ROLE_LOG")"
}

test_load_normalized_dataset_properties_fails_closed_when_a_reread_is_not_one_record() {
	set +e
	for l_answer in 'user:other\tv\tlocal\n' 'user:note\tv\tsideways\n' 'user:note\tv\n' ''; do
		(
			ZXFER_TEST_PROPERTY_ROWS=$(printf 'tank/src\tuser:note\ta\\nb\tlocal')
			zxfer_run_zfs_cmd_for_role() {
				case "$*" in
				*" user:note tank/src") printf '%b' "$l_answer" ;;
				*) zxfer_property_test_model_zfs "$@" ;;
				esac
			}
			zxfer_load_normalized_dataset_properties "tank/src" source
			printf 'status=%s list=<%s>\n%s\n' "$?" "$g_zxfer_normalized_dataset_properties" \
				"$g_zxfer_property_error_result"
		) >"$TEST_TMPDIR/normalized_reread_shape.out"
		assertEquals "a lone read answering [$l_answer] must fail closed" "status=1 list=<>
Failed to read property [user:note] of dataset [tank/src]: zfs get printed no single [user:note] record" \
			"$(cat "$TEST_TMPDIR/normalized_reread_shape.out")"
	done
}

test_load_normalized_dataset_properties_leaves_out_a_user_property_removed_before_its_lone_read() {
	ROLE_LOG="$TEST_TMPDIR/role_removed.log"
	: >"$ROLE_LOG"
	(
		# The multi-line mountpoint sends every later property to a lone
		# read. user:k is listed, then removed before its lone read, which
		# zfs answers with "user:k<TAB>-<TAB>-". casesensitivity keeps its
		# "-" source, and user:dash its value "-".
		ZXFER_TEST_PROPERTY_ROWS=$(printf '%s\t%s\t%s\t%s\n' \
			tank/src mountpoint '/mnt/a\nb' local \
			tank/src casesensitivity sensitive - \
			tank/src user:k real local \
			tank/src user:dash - local \
			tank/src zz:tail t local)
		ZXFER_TEST_PROPERTY_LATER_ROWS=$(printf '%s\n' "$ZXFER_TEST_PROPERTY_ROWS" |
			sed 's/^\(tank\/src	user:k	\)real	local$/\1-	-/')
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_load_normalized_dataset_properties "tank/src" source
		printf 'status=%s list=%s\n' "$?" "$g_zxfer_normalized_dataset_properties"
	) >"$TEST_TMPDIR/normalized_removed.out"
	assertEquals "The removed user property is left out; native and local \"-\" stay." \
		"status=0 list=mountpoint=/mnt/a%0Ab=local,casesensitivity=sensitive=-,user:dash=-=local,zz:tail=t=local" \
		"$(cat "$TEST_TMPDIR/normalized_removed.out")"
	assertEquals "user:k was read alone in both views before it was left out." \
		2 "$(grep -c -- '-- user:k tank/src$' "$ROLE_LOG")"
}

################################################################################
# RECURSIVE PREFETCH
################################################################################

# Purpose: Model a tank/src tree (with an unwanted sibling) and a backup/dst
# tree; tank/src's quota shows as none in the human view.
# Usage: zxfer_property_test_model_trees
zxfer_property_test_model_trees() {
	ZXFER_TEST_PROPERTY_ROWS=$(printf '%s\t%s\t%s\t%s\n' \
		tank/src quota 1073741824 local tank/src compression lz4 local \
		tank/src/child compression gzip 'inherited from tank/src' \
		tank/srcother compression off local \
		backup/dst compression lz4 local \
		backup/dst/child compression lz4 'inherited from backup/dst')
	ZXFER_TEST_PROPERTY_HUMAN_ROWS=$(printf '%s\n' "$ZXFER_TEST_PROPERTY_ROWS" |
		sed 's/^\(tank\/src	quota	\)1073741824/\1none/')
}

test_prefetch_recursive_normalized_properties_keeps_dataset_names_with_spaces() {
	(
		ZXFER_TEST_PROPERTY_ROWS=$(printf '%s\t%s\t%s\t%s\n' \
			tank/src compression lz4 local "tank/src/my child" compression gzip local)
		g_zxfer_source_property_tree_prefetch_root="tank/src"
		g_recursive_source_list="tank/src
tank/src/my child"
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_prefetch_recursive_normalized_properties source
		zxfer_property_table_find_dataset source "tank/src/my child"
		printf 'status=%s list=%s\n' "$?" "$g_zxfer_property_table_lookup_result"
	) >"$TEST_TMPDIR/prefetch_spaces.out"
	assertEquals "The wanted-dataset filter holds one dataset per line." \
		"status=0 list=compression=gzip=local" "$(cat "$TEST_TMPDIR/prefetch_spaces.out")"
}

test_prefetch_recursive_normalized_properties_leaves_datasets_with_multiline_values_to_live_reads() {
	ROLE_LOG="$TEST_TMPDIR/prefetch_multiline.log"
	: >"$ROLE_LOG"
	(
		# tank/src/b's note spans two lines; tank/src/c's note carries a line
		# shaped like tank/src/d's record; tank/src/e is plain.
		ZXFER_TEST_PROPERTY_ROWS=$(printf '%s\t%s\t%s\t%s\n' \
			tank/src compression lz4 local \
			tank/src/b compression lz4 'inherited from tank/src' \
			tank/src/b user:note 'line1\nline2' local \
			tank/src/c user:note 'v\tlocal\ntank/src/d\tcompression\toff' local \
			tank/src/d compression lz4 'inherited from tank/src' \
			tank/src/e compression lz4 'inherited from tank/src')
		g_zxfer_source_property_tree_prefetch_root="tank/src"
		g_recursive_source_list="tank/src
tank/src/b
tank/src/c
tank/src/d
tank/src/e"
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_prefetch_recursive_normalized_properties source
		printf 'status=%s state=%s table=<%s>\n' "$?" "$g_zxfer_source_property_tree_prefetch_state" \
			"$(zxfer_property_test_table_dump source)"
		for l_dataset in tank/src/b tank/src/c tank/src/d tank/src/e; do
			zxfer_load_normalized_dataset_properties "$l_dataset" source
			printf '%s hit=%s: %s\n' "$l_dataset" "$g_zxfer_normalized_dataset_properties_cache_hit" \
				"$g_zxfer_normalized_dataset_properties"
		done
	) >"$TEST_TMPDIR/prefetch_multiline.out"
	assertEquals "Only datasets before the first multi-line record are published; the rest read live and exactly." \
		"status=0 state=1 table=<tank/src	compression=lz4=local>
tank/src/b hit=0: compression=lz4=inherited from tank/src,user:note=line1%0Aline2=local
tank/src/c hit=0: user:note=v%09local%0Atank/src/d%09compression%09off=local
tank/src/d hit=0: compression=lz4=inherited from tank/src
tank/src/e hit=0: compression=lz4=inherited from tank/src" \
		"$(cat "$TEST_TMPDIR/prefetch_multiline.out")"
	assertEquals "The per-dataset fallback reads only what is ambiguous alone." \
		"source get -Ho property,value,source -- user:note tank/src/b
source get -Hpo property,value,source -- user:note tank/src/b
source get -Ho property,value,source -- user:note tank/src/c
source get -Hpo property,value,source -- user:note tank/src/c" \
		"$(grep -v ' all ' "$ROLE_LOG")"
}

test_prefetch_recursive_normalized_properties_never_trusts_a_copy_after_a_listed_record_the_views_lack() {
	(
		# origin appears on tank/src after the value views and before the
		# name list; tank/src/b's last value copies the listing's tail with
		# readonly=on.
		l_copy='v\ntank/src\torigin\tq@s\t-\ntank/src\treadonly\ton\tlocal\ntank/src/b\treadonly\ton\tlocal\ntank/src/b\tu:x\tz'
		ZXFER_TEST_PROPERTY_ROWS=$(printf '%s\t%s\t%s\t%s\n' \
			tank/src mounted yes - \
			tank/src readonly off default \
			tank/src/b readonly off default \
			tank/src/b u:x "$l_copy" local)
		ZXFER_TEST_PROPERTY_LATER_ROWS=$(printf '%s\t%s\t%s\t%s\n' \
			tank/src mounted yes - \
			tank/src origin 'q@s' - \
			tank/src readonly off default \
			tank/src/b readonly off default \
			tank/src/b u:x "$l_copy" local)
		g_zxfer_source_property_tree_prefetch_root="tank/src"
		g_recursive_source_list="tank/src
tank/src/b"
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_prefetch_recursive_normalized_properties source
		printf 'status=%s table=<%s>\n' "$?" "$g_zxfer_source_property_table"
		zxfer_load_normalized_dataset_properties tank/src source
		printf 'src=%s\n' "$g_zxfer_normalized_dataset_properties"
		zxfer_load_normalized_dataset_properties tank/src/b source
		printf 'b=%s\n' "$g_zxfer_normalized_dataset_properties"
	) >"$TEST_TMPDIR/prefetch_listed_missing.out"
	assertEquals "Nothing is published from the copy; each dataset is read live and exactly." \
		"status=0 table=<>
src=mounted=yes=-,origin=q@s=-,readonly=off=default
b=readonly=off=default,u:x=v%0Atank/src%09origin%09q@s%09-%0Atank/src%09readonly%09on%09local%0Atank/src/b%09readonly%09on%09local%0Atank/src/b%09u:x%09z=local" \
		"$(cat "$TEST_TMPDIR/prefetch_listed_missing.out")"
}

test_prefetch_recursive_normalized_properties_never_trusts_a_copy_when_a_later_dataset_gains_a_property() {
	(
		# u:new appears on tank/src/d after the value views and before the
		# name list; tank/src's u:x copies the listing's tail with readonly=on
		# for tank/src/b and tank/src/d.
		l_copy='v\tlocal\ntank/src/b\treadonly\ton\tlocal\ntank/src/d\treadonly\ton\tlocal\ntank/src/d\tu:y\ty\tlocal\ntank/src/d\tu:new\tz'
		ZXFER_TEST_PROPERTY_ROWS=$(printf '%s\t%s\t%s\t%s\n' \
			tank/src readonly off default \
			tank/src u:x "$l_copy" local \
			tank/src/b readonly off default \
			tank/src/d readonly off default \
			tank/src/d u:y y local)
		ZXFER_TEST_PROPERTY_LATER_ROWS=$(printf '%s\n%s\t%s\t%s\t%s\n' \
			"$ZXFER_TEST_PROPERTY_ROWS" tank/src/d u:new z local)
		g_zxfer_source_property_tree_prefetch_root="tank/src"
		g_recursive_source_list="tank/src
tank/src/b
tank/src/d"
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_prefetch_recursive_normalized_properties source
		printf 'status=%s table=<%s>\n' "$?" "$g_zxfer_source_property_table"
		for l_dataset in tank/src/b tank/src/d; do
			zxfer_load_normalized_dataset_properties "$l_dataset" source
			printf '%s: %s\n' "$l_dataset" "$g_zxfer_normalized_dataset_properties"
		done
	) >"$TEST_TMPDIR/prefetch_gained.out"
	assertEquals "The copied lines repeat real heads, so nothing is published from them." \
		"status=0 table=<>
tank/src/b: readonly=off=default
tank/src/d: readonly=off=default,u:y=y=local,u:new=z=local" \
		"$(cat "$TEST_TMPDIR/prefetch_gained.out")"
}

test_prefetch_recursive_normalized_properties_never_lists_a_dataset_destroyed_before_the_values() {
	(
		# tank/src's last value copies tank/src/b's records with readonly=on,
		# and tank/src/b is destroyed just before the first value view.
		ZXFER_TEST_PROPERTY_ROWS=$(printf '%s\t%s\t%s\t%s\n' \
			tank/src readonly off default \
			tank/src u:x 'v\tlocal\ntank/src/b\treadonly\ton\tlocal\ntank/src/b\tu:y\tq' local \
			tank/src/b readonly off default \
			tank/src/b u:y q local \
			tank/src/c readonly off default)
		l_rows_after=$(printf '%s\n' "$ZXFER_TEST_PROPERTY_ROWS" | grep -v '^tank/src/b	')
		g_zxfer_source_property_tree_prefetch_root="tank/src"
		g_recursive_source_list="tank/src
tank/src/b
tank/src/c"
		zxfer_run_zfs_cmd_for_role() {
			case "$*" in
			*" -Hpo "*" all "*) ZXFER_TEST_PROPERTY_ROWS=$l_rows_after ;;
			esac
			zxfer_property_test_model_zfs "$@"
		}
		zxfer_prefetch_recursive_normalized_properties source
		printf 'status=%s table=<%s>\n' "$?" "$g_zxfer_source_property_table"
		zxfer_load_normalized_dataset_properties tank/src/b source
		printf 'b status=%s list=<%s> error=%s\n' "$?" "$g_zxfer_normalized_dataset_properties" \
			"$g_zxfer_property_error_result"
		zxfer_load_normalized_dataset_properties tank/src source
		printf 'src=%s\n' "$g_zxfer_normalized_dataset_properties"
	) >"$TEST_TMPDIR/prefetch_destroyed.out"
	assertEquals "The copy of a destroyed dataset is never published; its live read fails closed." \
		"status=0 table=<>
b status=1 list=<> error=cannot open 'tank/src/b': dataset does not exist
src=readonly=off=default,u:x=v%09local%0Atank/src/b%09readonly%09on%09local%0Atank/src/b%09u:y%09q=local" \
		"$(cat "$TEST_TMPDIR/prefetch_destroyed.out")"
}

test_prefetch_recursive_normalized_properties_handles_invalid_side_and_state_shortcuts() {
	set +e
	(
		zxfer_run_zfs_cmd_for_role() {
			printf 'unexpected zfs call\n' >&2
			exit 99
		}
		zxfer_prefetch_recursive_normalized_properties other
		printf 'other=%s\n' "$?"
		g_zxfer_source_property_tree_prefetch_state=1
		zxfer_prefetch_recursive_normalized_properties source
		printf 'done=%s\n' "$?"
		g_zxfer_source_property_tree_prefetch_state=2
		zxfer_prefetch_recursive_normalized_properties source
		printf 'failed=%s\n' "$?"
		g_zxfer_source_property_tree_prefetch_state=0
		g_zxfer_source_property_tree_prefetch_root=""
		zxfer_prefetch_recursive_normalized_properties source
		printf 'no_root=%s state=%s\n' "$?" "$g_zxfer_source_property_tree_prefetch_state"
		g_zxfer_destination_property_tree_prefetch_root="backup/dst"
		g_recursive_dest_list=""
		zxfer_prefetch_recursive_normalized_properties destination
		printf 'no_datasets=%s state=%s\n' "$?" "$g_zxfer_destination_property_tree_prefetch_state"
	) >"$TEST_TMPDIR/prefetch_shortcuts.out"
	assertEquals "other=1
done=0
failed=1
no_root=1 state=2
no_datasets=1 state=2" "$(cat "$TEST_TMPDIR/prefetch_shortcuts.out")"
}

test_prefetch_recursive_normalized_properties_fails_closed_when_rows_cannot_be_stored() {
	ROLE_LOG="$TEST_TMPDIR/prefetch_unstored.log"
	: >"$ROLE_LOG"
	(
		zxfer_property_test_model_trees
		g_zxfer_source_property_tree_prefetch_root="tank/src"
		g_recursive_source_list="tank/src
tank/src/child"
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_prepare_property_read_files
		g_zxfer_property_row_dir="$TEST_TMPDIR/no-such-row-dir"
		l_status=ok
		zxfer_prefetch_recursive_normalized_properties source 2>/dev/null || l_status=failed
		printf 'prefetch=%s state=%s table=<%s>\n' "$l_status" \
			"$g_zxfer_source_property_tree_prefetch_state" "${g_zxfer_source_property_table:-}"
		zxfer_load_normalized_dataset_properties tank/src/child source
		printf 'child=%s hit=%s\n' "$g_zxfer_normalized_dataset_properties" \
			"$g_zxfer_normalized_dataset_properties_cache_hit"
		zxfer_load_normalized_dataset_properties tank/src/child source
		printf 'again hit=%s table=<%s>\n' "$g_zxfer_normalized_dataset_properties_cache_hit" \
			"${g_zxfer_source_property_table:-}"
	) >"$TEST_TMPDIR/prefetch_unstored.out"
	assertEquals "A store that cannot be written publishes nothing; every lookup reads live." \
		"prefetch=failed state=2 table=<>
child=compression=gzip=inherited from tank/src hit=0
again hit=0 table=<>" "$(cat "$TEST_TMPDIR/prefetch_unstored.out")"
	assertEquals "One tree read, then two live reads of three calls each." \
		9 "$(wc -l <"$ROLE_LOG" | tr -d ' ')"
}

test_property_row_store_guards_fail_closed() {
	(
		zxfer_property_test_table_add destination "backup/dst" "compression=lz4=local"
		l_index=$g_zxfer_destination_property_table
		# A name holding a TAB or LF is never in an index: nothing to drop.
		zxfer_invalidate_destination_property_mutation_cache "$(printf 'backup/dst\tx')" exact
		zxfer_invalidate_destination_property_mutation_cache "$(printf 'x\nbackup/dst')"
		[ "$g_zxfer_destination_property_table" = "$l_index" ] && printf 'odd names: unchanged\n'
		# A live read of such a name is used but never cached.
		zxfer_run_zfs_cmd_for_role() {
			case "$*" in
			*" property,value,source all "*) printf 'compression\tlz4\tlocal\n' ;;
			*) printf 'compression\n' ;;
			esac
		}
		zxfer_load_normalized_dataset_properties "$(printf 'tank/a\tb')" source
		printf 'odd live read: %s table=<%s>\n' "$g_zxfer_normalized_dataset_properties" \
			"${g_zxfer_source_property_table:-}"
		# A parse whose last line names no stored row publishes nothing.
		zxfer_parse_property_views() { printf 'x\ttank/src\n'; }
		g_zxfer_source_property_tree_prefetch_root="tank/src"
		g_recursive_source_list="tank/src"
		l_status=0
		zxfer_prefetch_recursive_normalized_properties source || l_status=$?
		printf 'bad parse: status=%s state=%s table=<%s>\n' "$l_status" \
			"$g_zxfer_source_property_tree_prefetch_state" "${g_zxfer_source_property_table:-}"
		# No row directory stops the run.
		zxfer_test_stub_throw_error_to_stdout
		zxfer_create_private_temp_dir() { return 1; }
		g_zxfer_property_row_dir=""
		zxfer_prepare_property_read_files
	) >"$TEST_TMPDIR/row_store_guards.out" 2>&1
	assertEquals "odd names: unchanged
odd live read: compression=lz4=local table=<>
bad parse: status=1 state=2 table=<>
Error creating temporary directory." "$(cat "$TEST_TMPDIR/row_store_guards.out")"
}

test_prefetch_recursive_normalized_properties_prepends_fresh_rows_ahead_of_live_rows() {
	(
		ZXFER_TEST_PROPERTY_ROWS=$(printf 'tank/src\tcompression\tlz4\tlocal')
		g_zxfer_source_property_tree_prefetch_root="tank/src"
		g_recursive_source_list="tank/src"
		zxfer_property_test_table_add source "tank/src" "compression=stale=local"
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_prefetch_recursive_normalized_properties source
		zxfer_property_table_find_dataset source tank/src
		printf '%s\n' "$g_zxfer_property_table_lookup_result"
	) >"$TEST_TMPDIR/prefetch_prepend.out"
	assertEquals "Fresh prefetch rows win over stale live rows." \
		"compression=lz4=local" "$(cat "$TEST_TMPDIR/prefetch_prepend.out")"
}

################################################################################
# REQUIRED-PROPERTY BACKFILL
################################################################################

test_backfill_required_properties_probes_each_property_when_the_batch_is_not_one_line_per_name() {
	for l_batch in 'casesensitivity\tsensitive\t-\n' \
		'casesensitivity\tsensitive\t-\nutf8only\toff\t-\nextra\tline\t-\n' \
		'utf8only\toff\t-\ncasesensitivity\tsensitive\t-\n'; do
		ROLE_LOG="$TEST_TMPDIR/backfill_shape.log"
		: >"$ROLE_LOG"
		(
			zxfer_run_zfs_cmd_for_role() {
				printf '%s\n' "$*" >>"$ROLE_LOG"
				case "$*" in
				*" casesensitivity,utf8only tank/src") printf '%b' "$l_batch" ;;
				*" casesensitivity tank/src") printf 'casesensitivity\tsensitive\t-\n' ;;
				*" utf8only tank/src") printf 'utf8only\toff\t-\n' ;;
				esac
			}
			zxfer_backfill_required_properties "tank/src" "" "casesensitivity,utf8only" source
			printf '%s reads=%s\n' "$g_zxfer_required_properties_result" "$(wc -l <"$ROLE_LOG" | tr -d ' ')"
		) >"$TEST_TMPDIR/backfill_shape.out"
		assertEquals "batch [$l_batch] falls back to one probe per name" \
			"casesensitivity=sensitive=-,utf8only=off=- reads=3" "$(cat "$TEST_TMPDIR/backfill_shape.out")"
	done
}

test_backfill_required_properties_takes_missing_values_from_the_sibling_list() {
	(
		zxfer_run_zfs_cmd_for_role() {
			printf 'unexpected zfs call\n' >&2
			exit 99
		}
		zxfer_backfill_required_properties "tank/src" "compression=lz4=local" "casesensitivity" source \
			"compression=lz4=local,casesensitivity=sensitive=-"
		printf '%s\n' "$g_zxfer_required_properties_result"
	) >"$TEST_TMPDIR/backfill_sibling.out"
	assertEquals "compression=lz4=local,casesensitivity=sensitive=-" "$(cat "$TEST_TMPDIR/backfill_sibling.out")"
}

test_backfill_required_properties_routes_single_probes_by_side_and_skips_inapplicable_properties() {
	ROLE_LOG="$TEST_TMPDIR/backfill_side.log"
	: >"$ROLE_LOG"
	(
		zxfer_run_zfs_cmd_for_role() {
			printf '%s\n' "$*" >>"$ROLE_LOG"
			printf 'property utf8only does not apply to datasets of this type\n' >&2
			return 1
		}
		zxfer_backfill_required_properties "pool/vol" "" "utf8only" destination
		printf 'status=%s result=<%s> error=<%s>\n' "$?" "$g_zxfer_required_properties_result" \
			"$g_zxfer_property_error_result"
	) >"$TEST_TMPDIR/backfill_side.out"
	assertEquals "status=0 result=<> error=<>" "$(cat "$TEST_TMPDIR/backfill_side.out")"
	assertEquals "destination get -Hpo property,value,source -- utf8only pool/vol" "$(cat "$ROLE_LOG")"
}

test_backfill_required_properties_reports_parse_failures_for_malformed_probe_output() {
	(
		zxfer_run_zfs_cmd_for_role() {
			printf 'garbage\n'
		}
		zxfer_backfill_required_properties "tank/src" "" "casesensitivity" source
		printf 'status=%s error=%s\n' "$?" "$g_zxfer_property_error_result"
	) >"$TEST_TMPDIR/backfill_parse_failure.out"
	assertEquals "status=1 error=Failed to retrieve required creation-time property [casesensitivity] for dataset [tank/src]: zfs get printed no single [casesensitivity] record" \
		"$(cat "$TEST_TMPDIR/backfill_parse_failure.out")"
}

test_backfill_required_properties_keeps_a_multiline_probe_value_whole() {
	(
		zxfer_run_zfs_cmd_for_role() {
			printf 'casesensitivity\ta\tlocal\nutf8only\tb\tlocal\n'
		}
		zxfer_backfill_required_properties "tank/src" "" "casesensitivity" source
		printf 'status=%s result=%s\n' "$?" "$g_zxfer_required_properties_result"
	) >"$TEST_TMPDIR/backfill_multiline.out"
	assertEquals "One probed record is cut at its last TAB, so a record-shaped value stays whole." \
		"status=0 result=casesensitivity=a%09local%0Autf8only%09b=local" \
		"$(cat "$TEST_TMPDIR/backfill_multiline.out")"
}

test_property_state_helpers_preserve_caller_ifs_and_globbing() {
	l_saved_ifs=$IFS
	IFS=","
	set -f
	zxfer_decode_property_value "a%2Cb"
	zxfer_backfill_required_properties "tank/src" "compression=lz4=local" "casesensitivity" source \
		"compression=lz4=local,casesensitivity=sensitive=-"
	l_globbing=$(zxfer_property_test_report_globbing_state after)
	l_ifs_after=$IFS
	set +f
	IFS=$l_saved_ifs

	assertEquals "a,b" "$g_zxfer_decoded_property_value"
	assertEquals "compression=lz4=local,casesensitivity=sensitive=-" "$g_zxfer_required_properties_result"
	assertEquals "after_globbing=disabled" "$l_globbing"
	assertEquals "," "$l_ifs_after"
}

# shellcheck source=tests/shunit2/shunit2
. "$SHUNIT2_BIN"
