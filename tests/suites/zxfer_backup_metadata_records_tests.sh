#!/bin/sh
# Backup metadata row buffering, capture, forwarded provenance, format
# validation, and write-boundary behavior tests.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2218,SC2317,SC2329

# Point the module at a private backup root under the physical temp dir and
# publish the exact-pair and forwarded file paths for tank/src -> backup/dst.
zxfer_backup_test_use_private_root() {
	g_backup_storage_root="$TEST_TMPDIR_PHYSICAL/$1"
	g_backup_file_extension=".zxfer_backup_info"
	g_zxfer_version="test-version"
	g_option_R_recursive="tank/src"
	g_option_N_nonrecursive=""
	BACKUP_TEST_PRIMARY_FILE="$g_backup_storage_root/tank/src/$(zxfer_get_backup_metadata_filename tank/src backup/dst)"
	BACKUP_TEST_FORWARDED_FILE="$g_backup_storage_root/backup/dst/src/$(zxfer_get_backup_metadata_filename backup/dst/src backup/dst/src)"
}

# Write one metadata file (0600 inside 0700 directories) with the given
# contents so restore and forwarded lookups can read it back.
zxfer_backup_test_write_file() {
	l_backup_test_path=$1
	l_backup_test_contents=$2

	(umask 077 && mkdir -p "${l_backup_test_path%/*}") || fail "Unable to create $l_backup_test_path parent."
	printf '%s\n' "$l_backup_test_contents" >"$l_backup_test_path"
	chmod 600 "$l_backup_test_path"
}

# Write the forwarded alias of ROOT (keyed ROOT/ROOT) with the given rows.
zxfer_backup_test_write_alias() {
	l_alias_test_root=$1
	shift
	ZXFER_TEST_BACKUP_SOURCE_ROOT=$l_alias_test_root
	ZXFER_TEST_BACKUP_DESTINATION_ROOT=$l_alias_test_root
	zxfer_backup_test_write_file \
		"$g_backup_storage_root/$l_alias_test_root/$(zxfer_get_backup_metadata_filename "$l_alias_test_root" "$l_alias_test_root")" \
		"$(zxfer_test_render_current_backup_metadata_contents "$@")"
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT ZXFER_TEST_BACKUP_DESTINATION_ROOT
}

test_reset_backup_metadata_state_clears_buffer_restore_and_forwarded_caches() {
	g_backup_file_contents="stale"
	g_restored_backup_file_contents="stale"
	g_zxfer_backup_file_read_result="stale"
	g_zxfer_backup_forwarded_roots="stale"
	g_zxfer_backup_forwarded_rows="stale"
	g_zxfer_backup_forwarded_properties="stale"
	g_zxfer_backup_forwarded_listed=1
	g_zxfer_backup_forwarded_listing="stale"

	zxfer_reset_backup_metadata_state

	assertEquals "" "$g_backup_file_contents"
	assertEquals "" "$g_restored_backup_file_contents"
	assertEquals "" "$g_zxfer_backup_file_read_result"
	assertEquals "The forwarded provenance memo must be rebuilt by the next run." \
		"" "$g_zxfer_backup_forwarded_roots$g_zxfer_backup_forwarded_rows$g_zxfer_backup_forwarded_properties$g_zxfer_backup_forwarded_listing"
	assertEquals "The -O listing must be taken again by the next run." 0 "$g_zxfer_backup_forwarded_listed"
}

test_backup_metadata_constants_pin_header_and_format_version() {
	assertEquals "#zxfer property backup file" "$ZXFER_BACKUP_METADATA_HEADER_LINE"
	assertEquals "2" "$ZXFER_BACKUP_METADATA_FORMAT_VERSION"
}

test_append_backup_metadata_record_keys_rows_relative_to_source_root() {
	g_backup_file_contents=""
	zxfer_append_backup_metadata_record "tank/src" "compression=lz4=local"
	zxfer_append_backup_metadata_record "tank/src/child" "atime=off=local"

	root_key=.
	expected_rows="$(zxfer_test_backup_metadata_row "$root_key" "compression=lz4=local")
$(zxfer_test_backup_metadata_row "child" "atime=off=local")"
	assertEquals "Rows are keyed by the source-root-relative path; the root row key is a dot." \
		"$expected_rows" "$g_backup_file_contents"

	output=$(
		(
			zxfer_append_backup_metadata_record "other/pool" "compression=lz4=local"
		) 2>&1
	)
	assertEquals "Datasets outside the source root must be refused." 1 "$?"
	assertContains "$output" "Backup metadata source dataset [other/pool] is outside source root [tank/src]."
}

test_append_backup_metadata_record_preserves_literal_backslashes_and_newline_rows() {
	g_backup_file_contents=$(zxfer_test_backup_metadata_row "." "user:note=a\\nb=local")
	zxfer_append_backup_metadata_record "tank/src/child" "user:path=C:\\temp=local"

	assertEquals "Appends must not interpret backslashes or collapse existing newline-separated rows." \
		"$(zxfer_test_backup_metadata_row "." "user:note=a\\nb=local")
$(zxfer_test_backup_metadata_row "child" "user:path=C:\\temp=local")" "$g_backup_file_contents"
}

test_validate_backup_metadata_record_list_collapses_duplicates_newest_wins_in_first_seen_order() {
	buffered="$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
$(zxfer_test_backup_metadata_row "child" "atime=off=local")
$(zxfer_test_backup_metadata_row "." "compression=off=local")"

	assertEquals "Duplicate keys collapse to the newest row while keeping first-appearance order." \
		"$(zxfer_test_backup_metadata_row "." "compression=off=local")
$(zxfer_test_backup_metadata_row "child" "atime=off=local")" \
		"$(zxfer_validate_backup_metadata_record_list "$buffered")"
}

test_validate_backup_metadata_record_list_rejects_malformed_rows() {
	for malformed in "no-tab-row" "$(zxfer_test_backup_metadata_row "." "")" \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local,")" \
		"$(zxfer_test_backup_metadata_row "" "compression=lz4=local")"; do
		output=$(zxfer_validate_backup_metadata_record_list "$malformed" 2>&1)
		assertEquals "Malformed row [$malformed] must fail validation." 1 "$?"
		assertEquals "A failed validation prints no partial list." "" "$output"
	done
}

test_capture_backup_metadata_for_completed_transfer_honours_mode_and_skip() {
	zxfer_backup_test_use_private_root capture_modes
	g_backup_file_contents=""

	g_option_k_backup_property_mode=0
	zxfer_capture_backup_metadata_for_completed_transfer "tank/src" "compression=lz4=local"
	assertEquals "Without -k nothing is buffered." "" "$g_backup_file_contents"

	g_option_k_backup_property_mode=1
	zxfer_capture_backup_metadata_for_completed_transfer "tank/src" "compression=lz4=local" 1
	assertEquals "The skip flag suppresses capture." "" "$g_backup_file_contents"

	zxfer_capture_backup_metadata_for_completed_transfer "tank/src/child" "compression=lz4=local"
	assertEquals "Without any alias the live row is buffered." \
		"$(zxfer_test_backup_metadata_row "child" "compression=lz4=local")" "$g_backup_file_contents"
}

test_capture_backup_metadata_for_completed_transfer_prefers_forwarded_rows_when_present() {
	g_backup_file_contents=""
	g_option_k_backup_property_mode=1

	output=$(
		(
			zxfer_resolve_forwarded_backup_metadata() {
				[ "$1" = tank/src ] || return 1
				g_zxfer_backup_forwarded_properties="compression=gzip=local"
			}
			zxfer_capture_backup_metadata_for_completed_transfer "tank/src" "compression=lz4=local"
			zxfer_capture_backup_metadata_for_completed_transfer "tank/src/child" "atime=off=local"
			printf '%s\n' "$g_backup_file_contents"
		)
	)

	assertEquals "A dataset with a forwarded row records the original provenance; one without keeps its live properties." \
		"$(zxfer_test_backup_metadata_row "." "compression=gzip=local")
$(zxfer_test_backup_metadata_row "child" "atime=off=local")" "$output"
}

test_resolve_forwarded_backup_metadata_uses_an_alias_below_the_source_root() {
	zxfer_backup_test_use_private_root forwarded_descendant
	zxfer_backup_test_write_alias tank/src/child \
		"$(zxfer_test_backup_metadata_row "." "compression=gzip=local")" \
		"$(zxfer_test_backup_metadata_row "grand" "compression=zstd=local")"

	zxfer_resolve_forwarded_backup_metadata tank/src
	assertEquals "The source root itself has no alias." 1 "$?"
	zxfer_resolve_forwarded_backup_metadata tank/src/child
	assertEquals "An alias below the source root is found for its own root." 0 "$?"
	assertEquals "compression=gzip=local" "$g_zxfer_backup_forwarded_properties"
	zxfer_resolve_forwarded_backup_metadata tank/src/child/grand
	assertEquals "Its descendants resolve through it." "compression=zstd=local" "$g_zxfer_backup_forwarded_properties"
}

test_resolve_forwarded_backup_metadata_prefers_the_nearest_alias() {
	zxfer_backup_test_use_private_root forwarded_nearest
	zxfer_backup_test_write_alias tank \
		"$(zxfer_test_backup_metadata_row "." "compression=off=local")" \
		"$(zxfer_test_backup_metadata_row "src" "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "src/child" "compression=lz4=local")"
	zxfer_backup_test_write_alias tank/src/child \
		"$(zxfer_test_backup_metadata_row "." "compression=gzip=local")"

	zxfer_resolve_forwarded_backup_metadata tank/src/child
	assertEquals "The alias at the dataset beats its ancestor's row." \
		"compression=gzip=local" "$g_zxfer_backup_forwarded_properties"
	zxfer_resolve_forwarded_backup_metadata tank/src
	assertEquals "A dataset above the nearer alias uses the ancestor alias." \
		"compression=lz4=local" "$g_zxfer_backup_forwarded_properties"
}

test_resolve_forwarded_backup_metadata_keeps_walking_when_an_alias_has_no_row() {
	zxfer_backup_test_use_private_root forwarded_no_row
	zxfer_backup_test_write_alias tank \
		"$(zxfer_test_backup_metadata_row "." "compression=off=local")" \
		"$(zxfer_test_backup_metadata_row "src/child" "compression=zstd=local")"
	zxfer_backup_test_write_alias tank/src \
		"$(zxfer_test_backup_metadata_row "." "compression=gzip=local")"

	zxfer_resolve_forwarded_backup_metadata tank/src/child
	assertEquals "The tank/src alias has no child row, so the tank alias answers." \
		"compression=zstd=local" "$g_zxfer_backup_forwarded_properties"
	zxfer_resolve_forwarded_backup_metadata tank/src/other
	assertEquals "No alias on the way up has a row for the dataset." 1 "$?"
	assertEquals "" "$g_zxfer_backup_forwarded_properties"
}

# A -k run whose -x excludes its own source root writes an alias with child
# rows and no "." row; it still forwards those rows. Regression: loading such
# an alias failed the run closed.
test_resolve_forwarded_backup_metadata_uses_aliases_without_their_own_root_row() {
	zxfer_backup_test_use_private_root forwarded_rootless
	zxfer_backup_test_write_alias tank/src/child \
		"$(zxfer_test_backup_metadata_row "grand" "compression=zstd=local")"
	zxfer_backup_test_write_alias tank \
		"$(zxfer_test_backup_metadata_row "src/child" "compression=gzip=local")"

	output=$(
		(
			for dataset in tank/src tank/src/child tank/src/child/grand; do
				if zxfer_resolve_forwarded_backup_metadata "$dataset"; then
					printf '%s=%s\n' "$dataset" "$g_zxfer_backup_forwarded_properties"
				else
					printf '%s=live\n' "$dataset"
				fi
			done
		) 2>&1
	)

	assertEquals "An alias without its own root row passes that root up the walk and forwards its other rows." \
		"tank/src=live
tank/src/child=compression=gzip=local
tank/src/child/grand=compression=zstd=local" "$output"
}

test_resolve_forwarded_backup_metadata_reads_each_root_at_most_once() {
	zxfer_backup_test_use_private_root forwarded_memo
	zxfer_backup_test_write_alias tank/src \
		"$(zxfer_test_backup_metadata_row "." "compression=gzip=local")" \
		"$(zxfer_test_backup_metadata_row "a" "compression=zstd=local")"
	# tank has a storage directory but no alias; the children have neither.
	read_log="$TEST_TMPDIR/forwarded_memo_reads.log"
	: >"$read_log"

	(
		READ_LOG=$read_log
		zxfer_read_local_backup_file() {
			printf '%s\n' "${1#"$g_backup_storage_root"/}" >>"$READ_LOG"
			[ -f "$1" ] || return 4
			g_zxfer_backup_file_read_result=$(cat "$1")
		}
		for dataset in tank/src tank/src/a tank/src/b tank/src/b/c tank/src/a; do
			zxfer_resolve_forwarded_backup_metadata "$dataset"
		done
	)

	assertEquals "Only roots with a storage directory are read (current then retired name), each once." \
		"tank/src/$(zxfer_get_backup_metadata_filename tank/src tank/src)
tank/$(zxfer_get_backup_metadata_filename tank tank)
tank/$(zxfer_get_backup_metadata_filename tank tank legacy)" "$(cat "$read_log")"
}

test_resolve_forwarded_backup_metadata_keeps_every_alias_it_reads_for_the_run() {
	zxfer_backup_test_use_private_root forwarded_two_aliases
	# A -N hop left a '.'-only alias at tank/src; an -R hop left a pool
	# alias with the child rows. Children walk past the first to the second.
	zxfer_backup_test_write_alias tank/src \
		"$(zxfer_test_backup_metadata_row "." "compression=gzip=local")"
	zxfer_backup_test_write_alias tank \
		"$(zxfer_test_backup_metadata_row "." "compression=off=local")" \
		"$(zxfer_test_backup_metadata_row "src/a" "compression=zstd=local")" \
		"$(zxfer_test_backup_metadata_row "src/b" "compression=lz4=local")"
	expected_rows="tank/src=compression=gzip=local
tank/src/a=compression=zstd=local
tank/src/b=compression=lz4=local
tank/src/a=compression=zstd=local"
	read_log="$TEST_TMPDIR/forwarded_two_aliases_reads.log"
	: >"$read_log"

	output=$(
		READ_LOG=$read_log
		zxfer_read_local_backup_file() {
			printf '%s\n' "${1#"$g_backup_storage_root"/}" >>"$READ_LOG"
			[ -f "$1" ] || return 4
			g_zxfer_backup_file_read_result=$(cat "$1")
		}
		for dataset in tank/src tank/src/a tank/src/b tank/src/a; do
			zxfer_resolve_forwarded_backup_metadata "$dataset"
			printf '%s=%s\n' "$dataset" "$g_zxfer_backup_forwarded_properties"
		done
	)
	assertEquals "$expected_rows" "$output"
	assertEquals "Locally each alias is read once." \
		"tank/src/$(zxfer_get_backup_metadata_filename tank/src tank/src)
tank/$(zxfer_get_backup_metadata_filename tank tank)" "$(cat "$read_log")"

	: >"$read_log"
	output=$(
		SSH_LOG=$read_log
		g_option_O_origin_host="origin.example"
		g_cmd_cat="/bin/cat"
		zxfer_build_remote_sh_c_command() {
			g_zxfer_remote_sh_c_command_result=$1
			printf '%s\n' "$1"
		}
		zxfer_invoke_ssh_shell_command_for_host() {
			if [ "${2#*find \'}" != "$2" ]; then
				printf 'listing\n' >>"$SSH_LOG"
			else
				printf 'read\n' >>"$SSH_LOG"
			fi
			sh -c "$2"
		}
		for dataset in tank/src tank/src/a tank/src/b tank/src/a; do
			zxfer_resolve_forwarded_backup_metadata "$dataset"
			printf '%s=%s\n' "$dataset" "$g_zxfer_backup_forwarded_properties"
		done
	)
	assertEquals "$expected_rows" "$output"
	assertEquals "Over -O: one listing, then one read per listed alias." \
		"listing
read
read" "$(cat "$read_log")"
}

test_resolve_forwarded_backup_metadata_over_origin_host_lists_once_then_reads_listed_roots() {
	zxfer_backup_test_use_private_root forwarded_remote
	zxfer_backup_test_write_alias tank/src/child \
		"$(zxfer_test_backup_metadata_row "." "compression=gzip=local")"
	ssh_log="$TEST_TMPDIR/forwarded_remote_ssh.log"
	: >"$ssh_log"

	output=$(
		(
			SSH_LOG=$ssh_log
			g_option_O_origin_host="origin.example"
			g_cmd_cat="/bin/cat"
			zxfer_build_remote_sh_c_command() {
				g_zxfer_remote_sh_c_command_result=$1
				printf '%s\n' "$1"
			}
			zxfer_invoke_ssh_shell_command_for_host() {
				# No case statement here: bash 3.2 (macOS /bin/sh)
				# mis-parses case patterns inside command substitution.
				if [ "${2#*find \'}" != "$2" ]; then
					printf 'listing\n' >>"$SSH_LOG"
				else
					printf 'read\n' >>"$SSH_LOG"
				fi
				sh -c "$2"
			}
			for dataset in tank/src tank/src/child tank/src/other tank/src/child; do
				if zxfer_resolve_forwarded_backup_metadata "$dataset"; then
					printf '%s=%s\n' "$dataset" "$g_zxfer_backup_forwarded_properties"
				else
					printf '%s=live\n' "$dataset"
				fi
			done
		) 2>&1
	)

	assertEquals "Remote lookups resolve like local ones." \
		"tank/src=live
tank/src/child=compression=gzip=local
tank/src/other=live
tank/src/child=compression=gzip=local" "$output"
	assertEquals "One listing, then reads only for roots with a storage directory (tank/src and tank: current and retired name; tank/src/child: found)." \
		"listing
read
read
read
read
read" "$(cat "$ssh_log")"
}

test_resolve_forwarded_backup_metadata_fails_closed_on_invalid_or_ambiguous_aliases() {
	zxfer_backup_test_use_private_root forwarded_invalid
	alias_file="$g_backup_storage_root/tank/src/$(zxfer_get_backup_metadata_filename tank/src tank/src)"

	zxfer_backup_test_write_file "$alias_file" "not a zxfer file"
	output=$(
		(
			zxfer_resolve_forwarded_backup_metadata tank/src
		) 2>&1
	)
	assertEquals "An invalid alias fails closed instead of recording live rows silently." 1 "$?"
	assertContains "$output" "Forwarded backup property file $alias_file does not start with the required zxfer backup metadata header."

	zxfer_backup_test_write_alias tank/src \
		"$(zxfer_test_backup_metadata_row "." "compression=gzip=local")" "broken-row"
	output=$(
		(
			zxfer_resolve_forwarded_backup_metadata tank/src
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "$output" "Forwarded backup property file $alias_file is malformed."

	zxfer_backup_test_write_alias tank/src \
		"$(zxfer_test_backup_metadata_row "." "compression=gzip=local")" \
		"$(zxfer_test_backup_metadata_row "child" "compression=off=local")" \
		"$(zxfer_test_backup_metadata_row "child" "compression=lz4=local")"
	output=$(
		(
			zxfer_resolve_forwarded_backup_metadata tank/src/child
		) 2>&1
	)
	assertEquals "Duplicate rows for the dataset fail closed." 1 "$?"
	assertContains "$output" "Forwarded backup property file $alias_file contains multiple relative rows for source dataset tank/src/child."
}

test_resolve_forwarded_backup_metadata_refuses_a_retired_name_alias_in_an_old_format() {
	zxfer_backup_test_use_private_root forwarded_retired
	legacy_file="$g_backup_storage_root/tank/src/.zxfer_backup_info.src.k1340387763.17"
	assertEquals "The retired name is keyed by cksum of 'tank/src<LF>tank/src'." \
		"$legacy_file" "$g_backup_storage_root/tank/src/$(zxfer_get_backup_metadata_filename tank/src tank/src legacy)"
	zxfer_backup_test_write_file "$legacy_file" "#zxfer property backup file
#format_version:1
tank/src,tank/src,compression=gzip"

	output=$(
		(
			zxfer_resolve_forwarded_backup_metadata tank/src/child
		) 2>&1
	)
	assertEquals "A v1 alias under the retired name fails closed." 1 "$?"
	assertContains "$output" "Forwarded backup property file $legacy_file does not declare supported zxfer backup metadata format version #format_version:2."
}

test_backup_metadata_extract_properties_for_dataset_pair_checks_header_and_version_first() {
	header=$ZXFER_BACKUP_METADATA_HEADER_LINE
	roots="#source_root:tank/src
#destination_root:backup/dst"
	row=$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
	for case_spec in \
		"0|$header
#format_version:2
$roots
$row" \
		"0|$header
#version:x
$roots
#format_version:2
$row" \
		"6|" \
		"6|$row" \
		"6|
$header
#format_version:2
$roots
$row" \
		"6|$header
$roots
$row
#format_version:2" \
		"6|$header
#format_version:2
$roots
$row
$header" \
		"7|$header" \
		"7|$header
$roots" \
		"7|$header
#format_version:1
$roots
$row" \
		"7|$header
#format_version:2
#format_version:2
$roots
$row" \
		"3|$header
#format_version:2
$roots
$row
broken-row" \
		"7|$header
#format_version:2
$roots
broken-row
#format_version:3" \
		"6|$header
#format_version:2
$roots
broken-row
$header" \
		"3|$header
#format_version:2
$row"; do
		expected=${case_spec%%|*}
		contents=${case_spec#*|}
		zxfer_backup_metadata_extract_properties_for_dataset_pair "$contents" tank/src backup/dst >/dev/null
		assertEquals "Contents [$contents] should return $expected." "$expected" "$?"
	done
}

test_backup_metadata_extract_properties_for_dataset_pair_resolves_relative_rows() {
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst/src"
	contents=$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=lz4=local")" \
		"$(zxfer_test_backup_metadata_row "child" "atime=off=local")")
	ambiguous=$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "a=1=local")" "$(zxfer_test_backup_metadata_row "." "a=2=local")")
	malformed=$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "a=1=local")" "tank/src,backup/dst,legacy=row")
	unset ZXFER_TEST_BACKUP_SOURCE_ROOT ZXFER_TEST_BACKUP_DESTINATION_ROOT

	assertEquals "compression=lz4=local" \
		"$(zxfer_backup_metadata_extract_properties_for_dataset_pair "$contents" tank/src backup/dst/src)"
	assertEquals "atime=off=local" \
		"$(zxfer_backup_metadata_extract_properties_for_dataset_pair "$contents" tank/src/child backup/dst/src/child)"
	zxfer_backup_metadata_extract_properties_for_dataset_pair "$contents" tank/src backup/other >/dev/null
	assertEquals "A destination outside the recorded root does not match." 1 "$?"
	zxfer_backup_metadata_extract_properties_for_dataset_pair "$contents" tank/src/missing backup/dst/src/missing >/dev/null
	assertEquals "A resolving pair without a row is told apart from an unresolved one." 8 "$?"
	zxfer_backup_metadata_extract_properties_for_dataset_pair "$ambiguous" tank/src backup/dst/src >/dev/null
	assertEquals "Duplicate rows are ambiguous." 2 "$?"
	zxfer_backup_metadata_extract_properties_for_dataset_pair "$malformed" tank/src backup/dst/src >/dev/null
	assertEquals "Legacy comma rows make the file malformed." 3 "$?"
}

test_write_backup_properties_skips_without_rows_and_publishes_both_files_once() {
	zxfer_backup_test_use_private_root write_once
	g_option_T_target_host=""
	g_option_n_dryrun=0
	g_option_v_verbose=1
	g_backup_file_contents=""

	output=$(zxfer_write_backup_properties 2>&1)
	assertEquals 0 "$?"
	assertEquals "Without rows (always the case under -n) the write only says so." \
		"No property data collected; skipping backup write." "$output"
	assertFalse "Nothing is created without rows." "[ -e '$g_backup_storage_root' ]"

	g_backup_file_contents="$(zxfer_test_backup_metadata_row "." "compression=lz4=local")
$(zxfer_test_backup_metadata_row "child" "atime=off=local")
$(zxfer_test_backup_metadata_row "." "compression=off=local")"
	zxfer_write_backup_properties >/dev/null
	assertEquals "The write succeeds." 0 "$?"

	for metadata_file in "$BACKUP_TEST_PRIMARY_FILE" "$BACKUP_TEST_FORWARDED_FILE"; do
		assertTrue "Metadata file must exist: $metadata_file" "[ -f '$metadata_file' ]"
		case "$(ls -ldn "$metadata_file")" in
		-rw-------*) ;;
		*) fail "Metadata must be 0600: $(ls -ldn "$metadata_file")" ;;
		esac
		case "$(ls -ldn "${metadata_file%/*}")" in
		drwx------*) ;;
		*) fail "Metadata directories must be 0700: $(ls -ldn "${metadata_file%/*}")" ;;
		esac
		assertEquals "The header line comes first." "$ZXFER_BACKUP_METADATA_HEADER_LINE" "$(sed -n '1p' "$metadata_file")"
		assertEquals "#format_version:2" "$(sed -n '2p' "$metadata_file")"
		assertEquals "Duplicate buffered rows collapse to one newest row per dataset." \
			1 "$(grep -c '^\.	' "$metadata_file")"
		assertTrue "grep -q '^\.	compression=off=local$' '$metadata_file'"
		assertTrue "grep -q '^child	atime=off=local$' '$metadata_file'"
		assertEquals "" "$(find "${metadata_file%/*}" -name '.zxfer-backup-write.*')"
	done
	assertTrue "grep -q '^#source_root:tank/src$' '$BACKUP_TEST_PRIMARY_FILE'"
	assertTrue "grep -q '^#destination_root:backup/dst/src$' '$BACKUP_TEST_PRIMARY_FILE'"
	assertTrue "grep -q '^#source_root:backup/dst/src$' '$BACKUP_TEST_FORWARDED_FILE'"
	assertTrue "grep -q '^#destination_root:backup/dst/src$' '$BACKUP_TEST_FORWARDED_FILE'"
	assertEquals "The write boundary leaves the buffer in its validated form." \
		"$(zxfer_test_backup_metadata_row "." "compression=off=local")
$(zxfer_test_backup_metadata_row "child" "atime=off=local")" "$g_backup_file_contents"
}

test_write_backup_properties_rejects_malformed_rows_before_touching_the_store() {
	zxfer_backup_test_use_private_root write_malformed
	g_option_T_target_host=""
	g_option_n_dryrun=0
	g_backup_file_contents="broken-row"

	output=$(
		(
			zxfer_write_backup_properties
		) 2>&1
	)
	assertEquals 1 "$?"
	assertContains "$output" "Failed to validate buffered backup metadata records"
	assertFalse "No directory or file may be created for malformed rows." "[ -e '$g_backup_storage_root' ]"
}
