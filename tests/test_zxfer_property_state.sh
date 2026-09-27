#!/bin/sh
#
# shunit2 tests for src/zxfer_property_state.sh: the untrusted zfs get
# parser, serialization, the in-memory property tables, normalized lookup
# routing, required-property backfill and the recursive prefetch.
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

test_parse_property_views_reads_one_line_records_on_the_fast_path() {
	result=$(zxfer_property_test_parse_merge 'user:note\nuser:tab\ncompression\n' \
		'user:note\tvalue,=; mix\tlocal\nuser:tab\ta\tb\tinherited from tank\ncompression\tlz4\tlocal\n')
	assertEquals "Every record is one line, so nothing is re-read; a value keeps its TABs." \
		"user:note=value%2C%3D%3B mix=local,user:tab=a%09b=inherited from tank,compression=lz4=local" "$result"
}

test_parse_property_views_leaves_a_multiline_value_to_be_reread() {
	result=$(zxfer_property_test_parse_merge 'compression\nuser:note\nuser:tail\n' \
		'compression\tlz4\tlocal\nuser:note\tline1\nline2\tlocal\nuser:tail\tt\tlocal\n')
	assertEquals "The run of known records ends at the multi-line one; it and every later record are re-read." \
		"compression=lz4=local,user:note,user:tail
user:note,user:tail" "$result"
}

test_parse_property_views_never_splits_a_record_shaped_value() {
	result=$(zxfer_property_test_parse_merge 'compression\nuser:record\n' \
		'compression\tlz4\tlocal\nuser:record\tx\tlocal\nuser:fake\ty\tlocal\n')
	assertEquals "A value shaped like a second record is re-read, never cut at the fake record." \
		"compression=lz4=local,user:record
user:record" "$result"
	assertNotContains "No property comes from a value line." "$result" "user:fake"
}

test_parse_property_views_rereads_values_that_mimic_real_property_names() {
	result=$(zxfer_property_test_parse_merge 'compression\nuser:a\nuser:b\n' \
		'compression\tlz4\tlocal\nuser:a\tx\tlocal\nuser:b\toff\tlocal\nuser:b\ton\tlocal\n')
	assertEquals "A continuation shaped like the next real record leaves both ambiguous." \
		"compression=lz4=local,user:a,user:b
user:a,user:b" "$result"

	result=$(zxfer_property_test_parse_merge 'compression\nuser:z\n' \
		'compression\tlz4\tlocal\nuser:z\tv\tlocal\nuser:z\tw\tlocal\n')
	assertEquals "A continuation that repeats its own record's name is ambiguous too." \
		"compression,user:z
compression,user:z" "$result"
}

test_parse_property_views_merges_the_human_none_in_skeleton_order() {
	result=$(zxfer_property_test_parse_merge 'quota\ncompression\n' \
		'quota\t1073741824\tlocal\ncompression\tlz4\tlocal\n' \
		'quota\tnone\tlocal\ncompression\tlz4\tlocal\n')
	assertEquals "quota=none=local,compression=lz4=local" "$result"
}

test_parse_property_views_rereads_when_a_view_disagrees_with_the_skeleton() {
	result=$(zxfer_property_test_parse_merge 'compression\natime\n' \
		'compression\tlz4\tlocal\natime\toff\tlocal\n' \
		'compression\tlz4\tlocal\nrecordsize\t128K\tdefault\n')
	assertEquals "A property one view lacks cannot be placed, so every record is re-read." \
		"compression,atime
compression,atime" "$result"
}

test_parse_property_views_ends_the_run_before_a_key_another_line_repeats() {
	result=$(zxfer_property_test_parse_merge 'compression\natime\nuser:x\n' \
		'compression\tlz4\tlocal\natime\toff\tlocal\nuser:x\tv\natime\ton\tlocal\n')
	assertEquals "atime heads two lines, so compression cannot be shown to end at line 1." \
		"compression,atime,user:x
compression,atime,user:x" "$result"
}

test_parse_property_views_never_trusts_a_copy_after_a_listed_record_the_views_lack() {
	result=$(zxfer_property_test_parse_merge 'mounted\norigin\nquota\nreadonly\nu:x\n' \
		'mounted\tyes\t-\nquota\tnone\tdefault\nreadonly\toff\tdefault\nu:x\tv\norigin\tpool/a@s\t-\nquota\tnone\tdefault\nreadonly\ton\tlocal\nu:x\tz\tlocal\n')
	assertEquals "Without origin the run ends at line 1, so the copy in u:x decides nothing." \
		"mounted,origin,quota,readonly,u:x
mounted,origin,quota,readonly,u:x" "$result"
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
	result=$(zxfer_parse_property_views prefetch "$TEST_TMPDIR/parse.wanted" \
		"$TEST_TMPDIR/parse.skeleton" "$TEST_TMPDIR/parse.machine" "$TEST_TMPDIR/parse.machine")
	assertEquals "p/a is published with user:car cut" \
		"p/a	type=filesystem=-,user:car=x=local
p/b	type=filesystem=-" "$result"
}

test_parse_property_views_preserves_literal_backslashes() {
	result=$(zxfer_property_test_parse_merge 'user:path\n' 'user:path\tC:\\temp\\new\tlocal\n')
	assertEquals 'user:path=C:\temp\new=local' "$result"
}

test_parse_property_views_fails_closed_on_a_bad_skeleton() {
	for l_skeleton in 'compression\ncompression\n' 'bad\trow\n' 'not a name\n' ''; do
		l_status=0
		result=$(zxfer_property_test_parse_merge "$l_skeleton" 'compression\tlz4\tlocal\n') ||
			l_status=$?
		assertEquals "skeleton [$l_skeleton] must fail the parse" 1 "$l_status"
		assertEquals "a failed parse prints nothing" "" "$result"
	done
	assertEquals "Empty views of an empty skeleton parse to nothing." \
		"" "$(zxfer_property_test_parse_merge '' '')"
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
}

test_decode_property_value_decodes_every_code_in_awk_order() {
	zxfer_decode_property_value "x%2Cy%3Dz%3B%25%09t%0Dr%0Al"
	assertEquals "$(printf 'x,y=z;%%\tt\rr\nl')" "$g_zxfer_decoded_property_value"
	zxfer_decode_property_value "%250A%2%2C%%3D"
	assertEquals "%25 decodes last, so an encoded percent never forms a new code." \
		"%0A%2,%=" "$g_zxfer_decoded_property_value"
}

test_decode_property_value_matches_the_awk_decoder() {
	l_awk_decoder="$ZXFER_PROPERTY_AWK_LIB"'
BEGIN { printf "%s", decode_value(ENVIRON["ZXFER_TEST_VALUE"]) }'
	for l_value in "" "plain" "%" "%%0A" "%0%0A" "%2%2C" "%3%3B" "%252C%2C" \
		"a%0D%0Ab" "%250D%0D" "%3D%3D=%3d" "tail%"; do
		l_awk_value=$(
			ZXFER_TEST_VALUE=$l_value "${g_cmd_awk:-awk}" "$l_awk_decoder"
			printf x
		)
		zxfer_decode_property_value "$l_value"
		assertEquals "decode of [$l_value]" "${l_awk_value%x}" "$g_zxfer_decoded_property_value"
	done
}

test_decode_property_value_keeps_control_bytes_and_needs_no_awk() {
	(
		g_cmd_awk="$TEST_TMPDIR/missing-awk"
		zxfer_decode_property_value "$(printf 'a\001-o\001b%%2C')"
		printf '%s' "$g_zxfer_decoded_property_value"
	) >"$TEST_TMPDIR/decode_plain.out"
	assertEquals "$(printf 'a\001-o\001b,')" "$(cat "$TEST_TMPDIR/decode_plain.out")"
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

test_zxfer_property_table_round_trips_hostile_dataset_names() {
	l_dataset="../unsafe path:/child"

	zxfer_property_test_table_add destination "$l_dataset" "user:note=line1%0Aline2=local"
	zxfer_property_test_table_add destination "backup/other" "compression=lz4=local"

	found=0
	zxfer_property_table_find_dataset destination "$l_dataset" && found=1
	payload=$g_zxfer_property_table_lookup_result
	zxfer_invalidate_destination_property_mutation_cache "$l_dataset" exact
	stale=0
	zxfer_property_table_find_dataset destination "$l_dataset" && stale=1
	survivor=0
	zxfer_property_table_find_dataset destination "backup/other" && survivor=1

	assertEquals "Hostile dataset names should round-trip through the table." 1 "$found"
	assertEquals "Rows should preserve their encoded payload exactly." \
		"user:note=line1%0Aline2=local" "$payload"
	assertEquals "Invalidation should remove rows keyed by hostile dataset names." 0 "$stale"
	assertEquals "Invalidation must not remove unrelated rows." 1 "$survivor"
}

test_zxfer_property_table_find_dataset_prefers_the_newest_row() {
	zxfer_property_test_table_add source tank/src "stale=1=local"
	zxfer_property_test_table_add source tank/src "fresh=1=local"
	zxfer_property_table_find_dataset source tank/src
	assertEquals "The newest row wins." "fresh=1=local" "$g_zxfer_property_table_lookup_result"
	zxfer_property_test_table_add source tank/src ""
	l_lookup_status=0
	zxfer_property_table_find_dataset source tank/src || l_lookup_status=$?
	assertEquals "An empty newest row must not expose an older duplicate." 1 "$l_lookup_status"
	assertEquals "A miss must clear the previous lookup." "" "$g_zxfer_property_table_lookup_result"
}

test_zxfer_property_table_find_dataset_reads_only_the_named_row() {
	zxfer_property_test_table_add source tank/src "compression=lz4=local"
	l_src_row=$g_zxfer_property_row_result
	zxfer_property_test_table_add source tank/src/a "compression=lz4=inherited from tank/src"
	zxfer_property_test_table_add source tank/src/b "atime=off=local"
	l_b_row=$g_zxfer_property_row_result
	# Replace tank/src's row with a directory: a lookup that scanned every
	# row would trip over it.
	rm -f "$g_zxfer_property_row_dir/$l_src_row"
	mkdir "$g_zxfer_property_row_dir/$l_src_row"
	zxfer_property_table_find_dataset source tank/src/a
	assertEquals "compression=lz4=inherited from tank/src" "$g_zxfer_property_table_lookup_result"
	# A row file cut short (no final LF) or missing is a miss, silently.
	printf 'atime=of' >"$g_zxfer_property_row_dir/$l_b_row"
	l_lookup_status=0
	l_stderr=$(zxfer_property_table_find_dataset source tank/src/b 2>&1) || l_lookup_status=$?
	assertEquals "A row without its final LF is a miss." "1:" "$l_lookup_status:$l_stderr"
	l_lookup_status=0
	l_stderr=$(zxfer_property_table_find_dataset source tank/src 2>&1) || l_lookup_status=$?
	assertEquals "An unreadable row is a miss." "1:" "$l_lookup_status:$l_stderr"
}

test_zxfer_find_property_row_matches_whole_keys_only() {
	l_index=$(printf '\np3\ttank/a b\np2\ttank/a\np1\t[x]*\n.')
	l_index=${l_index%.}
	zxfer_find_property_row "$l_index" "tank/a"
	assertEquals "p2" "$g_zxfer_property_row_result"
	zxfer_find_property_row "$l_index" "[x]*"
	assertEquals "Glob bytes in a key are literal." "p1" "$g_zxfer_property_row_result"
	for l_key in tank "a b" "tank/a b/c" "[x]" "*" ""; do
		assertFalse "[$l_key] names no line." "zxfer_find_property_row \"\$l_index\" \"\$l_key\""
	done
	# Without the TAB and LF guard this key would match across two lines
	# and answer with tank/a b's row.
	l_key=$(printf 'tank/a b\np2\ttank/a')
	assertFalse "A key spanning lines never matches." \
		"zxfer_find_property_row \"\$l_index\" \"\$l_key\""
}

test_zxfer_property_table_find_dataset_preserves_literal_keys_and_encoded_payloads() {
	l_payload='user:note=  literal\path%09tab%0Aline%25%2C%3D  =local'
	for l_dataset in 'tank/src/first' 'tank/src/[literal]*\name' 'tank/src/last'; do
		zxfer_property_test_table_add source "$l_dataset" "$l_payload"
	done
	for l_dataset in 'tank/src/first' 'tank/src/[literal]*\name' 'tank/src/last'; do
		l_lookup_status=0
		zxfer_property_table_find_dataset source "$l_dataset" || l_lookup_status=$?
		assertEquals "The exact key should match at any row position." 0 "$l_lookup_status"
		assertEquals "Lookup must preserve the encoded payload byte for byte." \
			"$l_payload" "$g_zxfer_property_table_lookup_result"
	done
}

test_zxfer_property_table_find_dataset_rejects_unknown_sides_and_misses() {
	zxfer_property_test_table_add source "tank/src" "compression=lz4=local"
	assertFalse "An unknown side must miss." "zxfer_property_table_find_dataset other tank/src"
	assertFalse "A prefix must not match a longer dataset name." "zxfer_property_table_find_dataset source tank/sr"
	assertFalse "The destination table must not answer source lookups." "zxfer_property_table_find_dataset destination tank/src"
}

test_zxfer_invalidate_destination_property_mutation_cache_strips_descendants_and_keeps_siblings() {
	zxfer_property_test_table_add destination "backup/dst" "compression=lz4=local"
	zxfer_property_test_table_add destination "backup/dst/child" "compression=gzip=inherited"
	zxfer_property_test_table_add destination "backup/dst2" "atime=off=local"
	g_zxfer_destination_property_tree_prefetch_state=1

	zxfer_invalidate_destination_property_mutation_cache "backup/dst"

	mutated=0
	zxfer_property_table_find_dataset destination "backup/dst" && mutated=1
	child=0
	zxfer_property_table_find_dataset destination "backup/dst/child" && child=1
	sibling=0
	zxfer_property_table_find_dataset destination "backup/dst2" && sibling=1

	assertEquals "The mutated dataset row must be invalidated." 0 "$mutated"
	assertEquals "Descendant rows must be invalidated (inherited values may have changed)." 0 "$child"
	assertEquals "Unrelated sibling rows must stay warm." 1 "$sibling"
	assertEquals "Targeted invalidation must keep the prefetched destination tree warm." \
		1 "$g_zxfer_destination_property_tree_prefetch_state"
}

test_zxfer_invalidate_destination_property_mutation_cache_without_dataset_resets_destination_table() {
	zxfer_property_test_table_add destination "backup/dst" "compression=lz4=local"
	zxfer_property_test_table_add source "tank/src" "compression=lz4=local"
	g_zxfer_destination_property_tree_prefetch_state=1

	zxfer_invalidate_destination_property_mutation_cache ""

	assertEquals "" "${g_zxfer_destination_property_table:-}"
	assertEquals "Source rows must survive a destination-wide reset." \
		"$(printf 'tank/src\tcompression=lz4=local')" "$(zxfer_property_test_table_dump source)"
	assertEquals "A destination-wide reset must re-arm the destination prefetch." \
		0 "$g_zxfer_destination_property_tree_prefetch_state"
}

test_zxfer_invalidate_destination_property_mutation_cache_exact_keeps_descendants_and_source_rows() {
	zxfer_property_test_table_add destination "backup/dst" "compression=lz4=local"
	zxfer_property_test_table_add destination "backup/dst/child" "compression=lz4=inherited"
	zxfer_property_test_table_add destination "backup/dst2" "atime=off=local"
	zxfer_property_test_table_add source "backup/dst" "compression=lz4=local"

	zxfer_invalidate_destination_property_mutation_cache "backup/dst" exact

	assertFalse "The exact row must be removed." "zxfer_property_table_find_dataset destination backup/dst"
	assertTrue "A receive leaves descendant rows warm." "zxfer_property_table_find_dataset destination backup/dst/child"
	assertTrue "Siblings must stay." "zxfer_property_table_find_dataset destination backup/dst2"
	assertTrue "Source rows are never invalidated." "zxfer_property_table_find_dataset source backup/dst"
}

test_zxfer_property_table_invalidation_clears_table_when_strip_command_fails() {
	zxfer_property_test_table_add destination "backup/dst" "compression=lz4=local"
	zxfer_property_test_table_add destination "backup/dst/child" "compression=lz4=inherited"
	(
		g_cmd_awk="$TEST_TMPDIR/missing-awk"
		zxfer_invalidate_destination_property_mutation_cache "backup/dst" 2>/dev/null
		printf '%s' "${g_zxfer_destination_property_table:-}"
	) >"$TEST_TMPDIR/strip_failure.out"
	assertEquals "A failed strip must clear the table so lookups fall back to live reads." \
		"" "$(cat "$TEST_TMPDIR/strip_failure.out")"
}

test_zxfer_invalidate_destination_property_mutation_cache_hides_one_dataset_without_awk() {
	zxfer_property_test_table_add destination "backup/dst" "compression=lz4=local"
	zxfer_property_test_table_add destination "backup/dst/child" "compression=lz4=inherited"
	(
		# A receive, or a create, set or inherit of a dataset without
		# descendant rows, needs no process: a tombstone hides the rows.
		g_cmd_awk="$TEST_TMPDIR/missing-awk"
		zxfer_invalidate_destination_property_mutation_cache "backup/dst" exact
		zxfer_invalidate_destination_property_mutation_cache "backup/dst/child"
		zxfer_invalidate_destination_property_mutation_cache "backup/none" exact
		zxfer_property_test_table_dump destination
		# The live read after the tombstone is the newest row again.
		zxfer_property_test_table_add destination "backup/dst" "compression=gzip=local"
		zxfer_property_table_find_dataset destination backup/dst
		printf 'after=%s\n' "$g_zxfer_property_table_lookup_result"
	) >"$TEST_TMPDIR/tombstone.out" 2>&1
	assertEquals "backup/dst/child	-
backup/dst	-
backup/dst/child	compression=lz4=inherited
backup/dst	compression=lz4=local
after=compression=gzip=local" "$(cat "$TEST_TMPDIR/tombstone.out")"
}

test_zxfer_reset_property_iteration_caches_clears_tables_and_prefetch_state() {
	zxfer_property_test_table_add source "tank/src" "compression=lz4=local"
	zxfer_property_test_table_add destination "backup/dst" "compression=lz4=local"
	g_zxfer_source_property_tree_prefetch_state=1
	g_zxfer_destination_property_tree_prefetch_root="backup/dst"
	g_zxfer_property_table_lookup_result="stale"

	zxfer_reset_property_iteration_caches

	assertEquals "" "$g_zxfer_source_property_table"
	assertEquals "" "$g_zxfer_destination_property_table"
	assertEquals 0 "$g_zxfer_source_property_tree_prefetch_state"
	assertEquals "" "$g_zxfer_destination_property_tree_prefetch_root"
	assertEquals "" "$g_zxfer_property_table_lookup_result"
}

test_zxfer_reset_destination_property_iteration_cache_preserves_source_table_and_rearms_destination_prefetch() {
	zxfer_property_test_table_add source "tank/src" "compression=lz4=local"
	zxfer_property_test_table_add destination "backup/dst" "compression=lz4=local"
	g_zxfer_destination_property_tree_prefetch_state=2

	zxfer_reset_destination_property_iteration_cache

	assertTrue "zxfer_property_table_find_dataset source tank/src"
	assertFalse "zxfer_property_table_find_dataset destination backup/dst"
	assertEquals 0 "$g_zxfer_destination_property_tree_prefetch_state"
}

test_zxfer_refresh_property_tree_prefetch_context_tracks_recursive_property_roots() {
	(
		g_option_R_recursive="-R"
		g_option_P_transfer_property=1
		g_initial_source="tank/src"
		g_destination="backup/dst"
		g_zxfer_source_property_tree_prefetch_state=2
		zxfer_refresh_property_tree_prefetch_context
		printf '%s|%s|%s|%s\n' "$g_zxfer_source_property_tree_prefetch_root" \
			"$g_zxfer_source_property_tree_prefetch_state" \
			"$g_zxfer_destination_property_tree_prefetch_root" \
			"$g_zxfer_destination_property_tree_prefetch_state"
	) >"$TEST_TMPDIR/prefetch_context.out"
	assertEquals "tank/src|0|backup/dst|0" "$(cat "$TEST_TMPDIR/prefetch_context.out")"
}

test_zxfer_refresh_property_tree_prefetch_context_clears_state_when_prefetch_is_inapplicable() {
	(
		g_option_R_recursive="-R"
		g_option_P_transfer_property=0
		g_option_o_override_property=""
		g_zxfer_source_property_tree_prefetch_root="stale"
		zxfer_refresh_property_tree_prefetch_context
		printf 'recursive_only=<%s>\n' "$g_zxfer_source_property_tree_prefetch_root"
		g_option_R_recursive=""
		g_option_o_override_property="compression=lz4"
		zxfer_refresh_property_tree_prefetch_context
		printf 'override_only=<%s>\n' "$g_zxfer_destination_property_tree_prefetch_root"
	) >"$TEST_TMPDIR/prefetch_context_clear.out"
	assertEquals "recursive_only=<>
override_only=<>" "$(cat "$TEST_TMPDIR/prefetch_context_clear.out")"
}

################################################################################
# NORMALIZED LOOKUP
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

# Purpose: Model tank/src and backup/dst with a quota the human view shows
# as none.
# Usage: zxfer_property_test_model_quota_rows
zxfer_property_test_model_quota_rows() {
	ZXFER_TEST_PROPERTY_ROWS=$(printf '%s\t%s\t%s\t%s\n' \
		tank/src quota 1073741824 local tank/src compression lz4 local \
		backup/dst quota 1073741824 local backup/dst compression lz4 local)
	ZXFER_TEST_PROPERTY_HUMAN_ROWS=$(printf '%s\t%s\t%s\t%s\n' \
		tank/src quota none local tank/src compression lz4 local \
		backup/dst quota none local backup/dst compression lz4 local)
}

test_load_normalized_dataset_properties_routes_live_probes_by_lookup_side() {
	ROLE_LOG="$TEST_TMPDIR/role_sides.log"
	: >"$ROLE_LOG"
	(
		zxfer_property_test_model_quota_rows
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_load_normalized_dataset_properties "tank/src" source
		printf '%s\n' "$g_zxfer_normalized_dataset_properties"
		zxfer_load_normalized_dataset_properties "backup/dst" destination
	) >"$TEST_TMPDIR/normalized_sides.out"
	assertEquals "The machine values merge in skeleton order with the human none." \
		"quota=none=local,compression=lz4=local" "$(cat "$TEST_TMPDIR/normalized_sides.out")"
	assertEquals "Each live read takes the machine and human views, then lists the names." \
		"source get -Hpo property,value,source all tank/src
source get -Ho property,value,source all tank/src
source get -Ho property all tank/src
destination get -Hpo property,value,source all backup/dst
destination get -Ho property,value,source all backup/dst
destination get -Ho property all backup/dst" "$(cat "$ROLE_LOG")"
}

test_load_normalized_dataset_properties_tracks_profile_counters_by_lookup_side() {
	(
		g_option_V_very_verbose=1
		zxfer_property_test_model_quota_rows
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_load_normalized_dataset_properties "tank/src" source
		zxfer_load_normalized_dataset_properties "backup/dst" destination
		zxfer_load_normalized_dataset_properties "backup/dst" destination
		printf 'source=%s destination=%s\n' \
			"${g_zxfer_profile_normalized_property_reads_source:-0}" \
			"${g_zxfer_profile_normalized_property_reads_destination:-0}"
	) >"$TEST_TMPDIR/normalized_profile.out" 2>/dev/null
	assertEquals "Live reads are counted per side; the cached repeat is not." \
		"source=1 destination=1" "$(cat "$TEST_TMPDIR/normalized_profile.out")"
}

test_load_normalized_dataset_properties_caches_same_side_dataset_and_separates_sides() {
	ROLE_LOG="$TEST_TMPDIR/role_cache.log"
	: >"$ROLE_LOG"
	(
		ZXFER_TEST_PROPERTY_ROWS=$(printf 'tank/src\tcompression\tlz4\tlocal')
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_load_normalized_dataset_properties "tank/src" source
		first_hit=$g_zxfer_normalized_dataset_properties_cache_hit
		zxfer_load_normalized_dataset_properties "tank/src" source
		second_hit=$g_zxfer_normalized_dataset_properties_cache_hit
		zxfer_load_normalized_dataset_properties "tank/src" destination
		third_hit=$g_zxfer_normalized_dataset_properties_cache_hit
		printf '%s %s %s %s\n' "$first_hit" "$second_hit" "$third_hit" "$(wc -l <"$ROLE_LOG" | tr -d ' ')"
	) >"$TEST_TMPDIR/normalized_cache.out"
	assertEquals "The second same-side lookup is a table hit; the other side re-reads." \
		"0 1 0 6" "$(cat "$TEST_TMPDIR/normalized_cache.out")"
}

test_load_normalized_dataset_properties_preserves_live_probe_status_and_diagnostic() {
	set +e
	(
		zxfer_run_zfs_cmd_for_role() {
			printf 'cannot open tank/src: permission denied\n' >&2
			return 3
		}
		zxfer_load_normalized_dataset_properties "tank/src" source
		status=$?
		printf 'status=%s error=%s cached=%s\n' "$status" "$g_zxfer_property_error_result" \
			"${g_zxfer_source_property_table:-<empty>}"
		exit "$status"
	) >"$TEST_TMPDIR/normalized_failure.out"
	status=$?
	assertEquals 3 "$status"
	assertEquals "status=3 error=cannot open tank/src: permission denied cached=<empty>" \
		"$(cat "$TEST_TMPDIR/normalized_failure.out")"
}

test_load_normalized_dataset_properties_reports_malformed_captures_without_caching() {
	set +e
	(
		zxfer_run_zfs_cmd_for_role() {
			printf 'garbage without fields\n'
		}
		zxfer_load_normalized_dataset_properties "tank/src" source
		status=$?
		printf 'status=%s cached=%s\n' "$status" "${g_zxfer_source_property_table:-<empty>}"
		printf '%s\n' "$g_zxfer_property_error_result"
	) >"$TEST_TMPDIR/normalized_malformed.out"
	assertEquals "status=1 cached=<empty>
Failed to parse the properties of dataset [tank/src]: the zfs get property list is malformed, repeats a name, or does not match the values." \
		"$(cat "$TEST_TMPDIR/normalized_malformed.out")"
}

test_load_normalized_dataset_properties_rereads_ambiguous_values_one_at_a_time() {
	ROLE_LOG="$TEST_TMPDIR/role_reread.log"
	: >"$ROLE_LOG"
	(
		# quota's human view says none; readonly is plain; user:record
		# reads like a record for user:fake; user:multi spans three lines.
		ZXFER_TEST_PROPERTY_ROWS=$(printf '%s\t%s\t%s\t%s\n' \
			tank/src quota '0' local \
			tank/src readonly off default \
			tank/src user:record 'x\tlocal\nuser:fake\ty' local \
			tank/src user:multi '\nmid\n' 'inherited from tank')
		ZXFER_TEST_PROPERTY_HUMAN_ROWS=$(printf '%s\n' "$ZXFER_TEST_PROPERTY_ROWS" |
			sed 's/^\(tank\/src	quota	\)0/\1none/')
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_load_normalized_dataset_properties "tank/src" source
		printf '%s\n' "$g_zxfer_normalized_dataset_properties"
	) >"$TEST_TMPDIR/normalized_reread.out"
	assertEquals "Ambiguous records are read alone and keep every byte; nothing is injected." \
		"quota=none=local,readonly=off=default,user:record=x%09local%0Auser:fake%09y=local,user:multi=%0Amid%0A=inherited from tank" \
		"$(cat "$TEST_TMPDIR/normalized_reread.out")"
	assertEquals "The first ambiguous record and every later one are read alone, human view first." \
		"source get -Ho property,value,source -- user:record tank/src
source get -Hpo property,value,source -- user:record tank/src
source get -Ho property,value,source -- user:multi tank/src
source get -Hpo property,value,source -- user:multi tank/src" \
		"$(grep -v ' all ' "$ROLE_LOG")"
}

test_load_normalized_dataset_properties_rereads_dash_named_properties_after_end_of_options() {
	ROLE_LOG="$TEST_TMPDIR/role_dash.log"
	: >"$ROLE_LOG"
	(
		# -x:m spans two lines; -x:y is plain but follows a multi-line value.
		# The model, like zfs, takes "-x:..." as an option unless -- comes first.
		ZXFER_TEST_PROPERTY_ROWS=$(printf '%s\t%s\t%s\t%s\n' \
			tank/src compression lz4 local \
			tank/src hn:desc 'Backups of\nthe web tier' local \
			tank/src -x:y one local \
			tank/src -x:m 'line1\nline2' local)
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_load_normalized_dataset_properties "tank/src" source
		printf 'status=%s list=%s\n%s\n' "$?" "$g_zxfer_normalized_dataset_properties" \
			"$g_zxfer_property_error_result"
	) >"$TEST_TMPDIR/normalized_dash.out"
	assertEquals "Dash-named properties are read alone and whole." \
		"status=0 list=compression=lz4=local,hn:desc=Backups of%0Athe web tier=local,-x:y=one=local,-x:m=line1%0Aline2=local" \
		"$(cat "$TEST_TMPDIR/normalized_dash.out")"
	assertEquals "Every lone read ends the options before the property name." \
		"source get -Ho property,value,source -- hn:desc tank/src
source get -Hpo property,value,source -- hn:desc tank/src
source get -Ho property,value,source -- -x:y tank/src
source get -Hpo property,value,source -- -x:y tank/src
source get -Ho property,value,source -- -x:m tank/src
source get -Hpo property,value,source -- -x:m tank/src" \
		"$(grep -v ' all ' "$ROLE_LOG")"
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

test_load_normalized_dataset_properties_fails_closed_when_a_reread_fails() {
	set +e
	(
		ZXFER_TEST_PROPERTY_ROWS=$(printf 'tank/src\tuser:note\ta\\nb\tlocal')
		zxfer_run_zfs_cmd_for_role() {
			case "$*" in
			*" user:note tank/src")
				printf 'permission denied\n' >&2
				return 7
				;;
			esac
			zxfer_property_test_model_zfs "$@"
		}
		zxfer_load_normalized_dataset_properties "tank/src" source
		status=$?
		printf 'status=%s list=<%s> cached=<%s>\n%s\n' "$status" \
			"$g_zxfer_normalized_dataset_properties" "${g_zxfer_source_property_table:-}" \
			"$g_zxfer_property_error_result"
	) >"$TEST_TMPDIR/normalized_reread_failure.out"
	assertEquals "status=7 list=<> cached=<>
Failed to read property [user:note] of dataset [tank/src]: permission denied" \
		"$(cat "$TEST_TMPDIR/normalized_reread_failure.out")"
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

test_load_normalized_dataset_properties_prefetches_recursive_source_tree_and_slices_locally() {
	ROLE_LOG="$TEST_TMPDIR/prefetch_source.log"
	: >"$ROLE_LOG"
	(
		zxfer_property_test_model_trees
		g_zxfer_source_property_tree_prefetch_root="tank/src"
		g_recursive_source_list="tank/src
tank/src/child"
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_load_normalized_dataset_properties "tank/src" source
		printf 'root=%s hit=%s\n' "$g_zxfer_normalized_dataset_properties" "$g_zxfer_normalized_dataset_properties_cache_hit"
		zxfer_load_normalized_dataset_properties "tank/src/child" source
		printf 'child=%s hit=%s\n' "$g_zxfer_normalized_dataset_properties" "$g_zxfer_normalized_dataset_properties_cache_hit"
		printf 'state=%s reads=%s\n' "$g_zxfer_source_property_tree_prefetch_state" "$(wc -l <"$ROLE_LOG" | tr -d ' ')"
		printf 'unwanted=%s\n' "$(zxfer_property_table_find_dataset source tank/srcother && echo cached || echo absent)"
	) >"$TEST_TMPDIR/prefetch_source.out"
	assertEquals "root=quota=none=local,compression=lz4=local hit=1
child=compression=gzip=inherited from tank/src hit=1
state=1 reads=3
unwanted=absent" "$(cat "$TEST_TMPDIR/prefetch_source.out")"
	assertEquals "The prefetch takes the machine and human views, then lists the names." \
		"source get -r -t filesystem,volume -Hpo name,property,value,source all tank/src
source get -r -t filesystem,volume -Ho name,property,value,source all tank/src
source get -r -t filesystem,volume -Ho name,property all tank/src" "$(cat "$ROLE_LOG")"
}

test_load_normalized_dataset_properties_prefetches_recursive_destination_tree_and_slices_locally() {
	ROLE_LOG="$TEST_TMPDIR/prefetch_destination.log"
	: >"$ROLE_LOG"
	(
		zxfer_property_test_model_trees
		g_zxfer_destination_property_tree_prefetch_root="backup/dst"
		g_recursive_dest_list="backup/dst
backup/dst/child"
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_model_zfs "$@"; }
		zxfer_load_normalized_dataset_properties "backup/dst/child" destination
		printf 'child=%s hit=%s state=%s reads=%s\n' "$g_zxfer_normalized_dataset_properties" \
			"$g_zxfer_normalized_dataset_properties_cache_hit" \
			"$g_zxfer_destination_property_tree_prefetch_state" "$(wc -l <"$ROLE_LOG" | tr -d ' ')"
	) >"$TEST_TMPDIR/prefetch_destination.out"
	assertEquals "child=compression=lz4=inherited from backup/dst hit=1 state=1 reads=3" \
		"$(cat "$TEST_TMPDIR/prefetch_destination.out")"
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

test_prefetch_recursive_normalized_properties_falls_back_to_live_reads_when_tree_read_fails() {
	ROLE_LOG="$TEST_TMPDIR/prefetch_failure.log"
	: >"$ROLE_LOG"
	(
		zxfer_property_test_model_quota_rows
		g_zxfer_source_property_tree_prefetch_root="tank/src"
		g_recursive_source_list="tank/src"
		zxfer_run_zfs_cmd_for_role() {
			case "$*" in
			*" get -r "*)
				printf '%s\n' "$*" >>"$ROLE_LOG"
				return 1
				;;
			esac
			zxfer_property_test_model_zfs "$@"
		}
		zxfer_load_normalized_dataset_properties "tank/src" source
		printf 'payload=%s state=%s\n' "$g_zxfer_normalized_dataset_properties" "$g_zxfer_source_property_tree_prefetch_state"
		zxfer_load_normalized_dataset_properties "tank/src/child" source
		printf 'recursive_attempts=%s\n' "$(grep -c ' get -r ' "$ROLE_LOG")"
	) >"$TEST_TMPDIR/prefetch_failure.out"
	assertEquals "A failed tree read marks the side failed once and every lookup falls back to live reads." \
		"payload=quota=none=local,compression=lz4=local state=2
recursive_attempts=1" "$(cat "$TEST_TMPDIR/prefetch_failure.out")"
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

test_prefetch_recursive_normalized_properties_fails_closed_on_a_malformed_skeleton() {
	(
		ZXFER_TEST_PROPERTY_ROWS=$(printf 'tank/src\tcompression\tlz4\tlocal')
		g_zxfer_source_property_tree_prefetch_root="tank/src"
		g_recursive_source_list="tank/src"
		zxfer_run_zfs_cmd_for_role() {
			case "$*" in
			*" -Ho name,property all "*) printf 'tank/src\n' ;;
			*) zxfer_property_test_model_zfs "$@" ;;
			esac
		}
		zxfer_prefetch_recursive_normalized_properties source
		printf 'status=%s state=%s table=<%s>\n' "$?" "$g_zxfer_source_property_tree_prefetch_state" "${g_zxfer_source_property_table:-}"
	) >"$TEST_TMPDIR/prefetch_malformed.out"
	assertEquals "status=1 state=2 table=<>" "$(cat "$TEST_TMPDIR/prefetch_malformed.out")"
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

test_backfill_required_properties_keeps_present_properties_without_probes() {
	(
		zxfer_run_zfs_cmd_for_role() {
			printf 'unexpected zfs call\n' >&2
			exit 99
		}
		zxfer_backfill_required_properties "tank/src" \
			"casesensitivity=sensitive=-,normalization=none=-,utf8only=off=-,compression=lz4=local" \
			"casesensitivity,normalization,utf8only" source
		printf '%s\n' "$g_zxfer_required_properties_result"
	) >"$TEST_TMPDIR/backfill_present.out"
	assertEquals "casesensitivity=sensitive=-,normalization=none=-,utf8only=off=-,compression=lz4=local" \
		"$(cat "$TEST_TMPDIR/backfill_present.out")"
}

test_backfill_required_properties_probes_missing_properties_with_one_comma_list_call() {
	ROLE_LOG="$TEST_TMPDIR/backfill_batch.log"
	: >"$ROLE_LOG"
	(
		g_option_V_very_verbose=1
		zxfer_run_zfs_cmd_for_role() {
			printf '%s\n' "$*" >>"$ROLE_LOG"
			case "$*" in
			"source get -Hpo property,value,source casesensitivity,utf8only tank/src")
				printf 'casesensitivity\tsensitive\t-\nutf8only\toff\t-\n'
				;;
			*) return 1 ;;
			esac
		}
		zxfer_backfill_required_properties "tank/src" "normalization=none=-,compression=lz4=local" \
			"casesensitivity,normalization,utf8only" source
		printf '%s\n' "$g_zxfer_required_properties_result"
		printf 'gets=%s\n' "${g_zxfer_profile_required_property_backfill_gets:-0}"
	) >"$TEST_TMPDIR/backfill_batch.out"
	assertEquals "normalization=none=-,compression=lz4=local,casesensitivity=sensitive=-,utf8only=off=-
gets=1" "$(cat "$TEST_TMPDIR/backfill_batch.out")"
	assertEquals "source get -Hpo property,value,source casesensitivity,utf8only tank/src" "$(cat "$ROLE_LOG")"
}

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

test_backfill_required_properties_falls_back_to_per_property_probes_when_the_batch_fails() {
	ROLE_LOG="$TEST_TMPDIR/backfill_fallback.log"
	: >"$ROLE_LOG"
	(
		g_option_V_very_verbose=1
		zxfer_run_zfs_cmd_for_role() {
			printf '%s\n' "$*" >>"$ROLE_LOG"
			case "$*" in
			"destination get -Hpo property,value,source -- casesensitivity backup/dst")
				printf 'casesensitivity\tsensitive\t-\n'
				;;
			"destination get -Hpo property,value,source -- normalization backup/dst")
				printf 'bad property list: invalid property normalization\n' >&2
				return 1
				;;
			*)
				printf 'bad property list\n' >&2
				return 1
				;;
			esac
		}
		zxfer_backfill_required_properties "backup/dst" "" "casesensitivity,normalization" destination
		printf 'status=%s result=%s gets=%s\n' "$?" "$g_zxfer_required_properties_result" \
			"${g_zxfer_profile_required_property_backfill_gets:-0}"
	) >"$TEST_TMPDIR/backfill_fallback.out"
	assertEquals "status=0 result=casesensitivity=sensitive=- gets=3" "$(cat "$TEST_TMPDIR/backfill_fallback.out")"
	assertEquals "destination get -Hpo property,value,source casesensitivity,normalization backup/dst
destination get -Hpo property,value,source -- casesensitivity backup/dst
destination get -Hpo property,value,source -- normalization backup/dst" "$(cat "$ROLE_LOG")"
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

test_backfill_required_properties_reports_probe_failures() {
	(
		zxfer_run_zfs_cmd_for_role() {
			printf 'permission denied\n' >&2
			return 4
		}
		zxfer_backfill_required_properties "tank/src" "" "casesensitivity" source
		printf 'status=%s error=%s\n' "$?" "$g_zxfer_property_error_result"
	) >"$TEST_TMPDIR/backfill_probe_failure.out"
	assertEquals "status=4 error=Failed to retrieve required creation-time property [casesensitivity] for dataset [tank/src]: permission denied" \
		"$(cat "$TEST_TMPDIR/backfill_probe_failure.out")"
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
