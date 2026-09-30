#!/bin/sh
#
# Black-box -k / -e property backup metadata suite for ./zxfer.
#
# Drives the real launcher against the canned zfs from
# tests/mock_toolchain_helper.sh (helpers in tests/helpers/blackbox.sh) and
# asserts on the metadata files -k writes (layout, modes, renames, rows),
# the properties -e restores, and the ssh programs -O and -T run for them.
#
# The invariant each test pins:
#
#   -k / -e property backup metadata contract (pinned 2026-09)
#       test_backup_mode_writes_metadata_once_per_run
#       → `-k -P` writes ZXFER_BACKUP_DIR/<source>/.zxfer_backup_info.v2/h/
#         <identity-chunks>/.zxfer_backup_info.v2 (mode 0600, versioned
#         header, one relative row per dataset) plus the forwarded
#         provenance alias under the destination root, with exactly ONE
#         rename per file for the whole run (no per-dataset rewrites), and
#         every directory it creates is 0700.
#       test_backup_mode_second_run_rewrites_metadata_in_place
#       → a second run replaces the file through one rename again and
#         leaves no stage files behind.
#       test_backup_mode_refuses_symlinked_backup_directory_and_target
#       → a symlinked ZXFER_BACKUP_DIR and a symlinked target file are both
#         refused with a non-zero exit and the link target untouched; so are
#         a directory at the metadata path and a symlinked alias, before
#         any rename.
#       test_restore_mode_rejects_legacy_layout_and_unsupported_format_version
#       → `-e` fails closed before any zfs argv when only a legacy flat
#         layout or the parent's exact-pair file exists (exit 1, the usage
#         text, the ZXFER_BACKUP_DIR hint) or the header declares an
#         unsupported version.
#       test_restore_mode_applies_recorded_properties
#       → `-e` reads the exact-pair file and `MUTATE set`s the recorded
#         value even when the live source has drifted.
#       test_remote_target_backup_mode_writes_metadata_through_ssh
#       → `-T -k` publishes both files through ONE rollback-capable pair
#         write script over the ssh transport, with the same layout and mode.
#       test_restore_mode_reads_a_current_file_under_the_retired_cksum_name
#       → `-e` still restores from the read-only retired name
#         .zxfer_backup_info.<tail>.k<cksum>.<length>.
#       test_backup_mode_refuses_a_v1_forwarded_alias_under_the_retired_name
#       → a v1 forwarded alias at the retired name fails `-k` closed and
#         writes nothing.
#       test_backup_mode_forwards_an_alias_below_the_source_root
#       → a forwarded alias for a child dataset supplies that child's row.
#       test_remote_origin_backup_mode_forwards_an_alias_below_the_source_root
#       → with -O the same alias is found through one listing of the
#         origin's storage directories, and each listed root is read once.
#       test_dry_run_backup_mode_previews_only_the_backup_root
#       → `-n -v -k -P` prints the mkdir/chmod preview and the no-data
#         note, issues no zfs argv, and creates nothing.
#       test_backup_mode_failure_partway_keeps_the_previous_files
#       → a `-k` run that fails at a later dataset leaves the previous
#         complete files byte-identical.
#       test_restore_mode_rejects_invalid_exact_pair_files_before_any_zfs_call
#       → `-e` refuses a file without the header, with a malformed row, with
#         two root rows, without the root row, or recorded for another
#         destination root: exit 1, that file's error, no zfs argv.
#       test_remote_origin_restore_mode_reads_the_exact_pair_file_through_ssh
#       → `-O -e` reads the file with one remote read program and sets the
#         recorded values; without the file it reads both names and stops.
#       test_remote_target_dry_run_backup_mode_previews_the_directory_program
#       → `-n -v -T -k -P` prints the directory program as one ssh line,
#         wrapper tokens kept, and runs nothing over ssh.
#       test_backup_mode_refuses_unsafe_backup_dir_roots_at_startup
#       → a relative ZXFER_BACKUP_DIR, or one with a TAB, CR or LF, stops
#         the run at startup with no zfs argv and is never created.
#       test_backup_mode_ignores_hostile_inherited_backup_state
#       → exported internal backup globals neither move the root (a dry run
#         without ZXFER_BACKUP_DIR previews /var/db/zxfer) nor add rows.
#       test_backup_mode_fails_closed_on_an_alias_with_duplicate_rows
#       → an alias with two rows for child1 fails `-k` closed and writes
#         nothing; a run without -k never reads the backup store.
#       test_backup_mode_forwards_the_nearest_alias_row_across_two_aliases
#       → the nearest alias with a row wins and an alias without the row
#         passes the dataset up, locally and over -O (each alias read once).
#       test_backup_mode_records_one_row_per_dataset_after_a_seeded_reconcile
#       → a seeded child is captured twice and recorded once; the post-seed
#         checkpoint and the run end each publish the pair.
#       test_backup_mode_records_hostile_property_values_literally
#       → $(...), backticks, backslashes and quotes in a value are recorded
#         verbatim, locally and over -T, and never run.
#       test_remote_target_backup_mode_keeps_a_wrapper_host_spec
#       → with -T 'localhost pfexec' the directory and write programs run
#         through the wrapper.
#
# shellcheck disable=SC1090,SC2034,SC2154

TESTS_DIR=$(dirname "$0")

# shellcheck source=tests/test_helper.sh
. "$TESTS_DIR/test_helper.sh"

# shellcheck source=tests/helpers/blackbox.sh
. "$TESTS_DIR/helpers/blackbox.sh"

# ---------------------------------------------------------------------------
# -k / -e property backup metadata contract. planning_backup_metadata_file
# (tests/helpers/blackbox.sh) derives the documented layout independently.

# Invariant (-k write-once): one live `-k -P` run over three datasets writes
# the exact-pair file and the forwarded alias exactly once each: two `mv`
# spawns for the whole run. The pre-2026-09 per-dataset flush cost 14+ mv
# and mktemp spawns on the same fixture.
test_backup_mode_writes_metadata_once_per_run() {
	planning_setup_backup_env k_write

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "-k -P no-op must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_backup_file_is_current_format "$PRIMARY_FILE" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	planning_assert_backup_file_is_current_format "$FORWARDED_FILE" \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "each metadata file is published by exactly one rename per run" \
		2 "$(grep -c '^mv$' "$SPAWN_LOG")"
	case "$(ls -ldn "$BACKUP_ROOT")" in
	drwx------*) ;;
	*) fail "the backup root zxfer creates must be mode 0700: $(ls -ldn "$BACKUP_ROOT")" ;;
	esac
	assertEquals "every directory zxfer creates under the backup root must be mode 0700" \
		"" "$(find "$BACKUP_ROOT" -type d ! -perm 700)"
}

# Invariant (-k rewrite): a second run over the same pair replaces both files
# through one rename each and keeps the single-row-per-dataset contract.
test_backup_mode_second_run_rewrites_metadata_in_place() {
	planning_setup_backup_env k_rewrite

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "first -k -P run must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	printf '#stale-marker\n' >>"$PRIMARY_FILE"

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "second -k -P run must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	planning_assert_backup_file_is_current_format "$PRIMARY_FILE" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	planning_assert_backup_file_is_current_format "$FORWARDED_FILE" \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "the rewrite is again one rename per file" \
		2 "$(grep -c '^mv$' "$SPAWN_LOG")"
	assertFalse "the rewrite must replace the previous file rather than append to it" \
		"grep -q '#stale-marker' '$PRIMARY_FILE'"
}

# Invariant (symlink guards): a symlinked ZXFER_BACKUP_DIR is refused before
# anything is written into its target, and a symlink planted at the exact
# metadata path is refused without following it.
test_backup_mode_refuses_symlinked_backup_directory_and_target() {
	planning_setup_backup_env k_symlink
	l_real_root="$CASE_DIR/backup_real"
	mkdir -p "$l_real_root"
	BACKUP_ROOT="$CASE_DIR/backup_link"
	ln -s "$l_real_root" "$BACKUP_ROOT"

	planning_run_backup_zxfer -k -P
	assertNotEquals "a symlinked backup directory must fail the run" 0 $?
	grep -q "Refusing to use backup directory" "$CASE_DIR/zxfer.stderr" ||
		fail "expected the symlinked backup directory refusal; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	assertEquals "nothing may be written through the symlinked root" \
		"" "$(find "$l_real_root" -type f)"
	planning_assert_no_mutations

	BACKUP_ROOT="$CASE_DIR/backup"
	PRIMARY_FILE=$(planning_backup_metadata_file "$BACKUP_ROOT" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT")
	l_decoy="$CASE_DIR/decoy"
	printf 'decoy\n' >"$l_decoy"
	mkdir -p "${PRIMARY_FILE%/*}"
	chmod 700 "${PRIMARY_FILE%/*}"
	ln -s "$l_decoy" "$PRIMARY_FILE"

	planning_run_backup_zxfer -k -P
	assertNotEquals "a symlinked metadata target must fail the run" 0 $?
	grep -q "Refusing to write backup metadata" "$CASE_DIR/zxfer.stderr" ||
		fail "expected the symlinked target refusal; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	assertEquals "the symlink target must be untouched" "decoy" "$(cat "$l_decoy")"
	assertTrue "the planted symlink must not be replaced" "[ -L '$PRIMARY_FILE' ]"
	assertEquals "no rename may run when the target is refused" \
		0 "$(grep -c '^mv$' "$SPAWN_LOG")"

	rm -f "$PRIMARY_FILE"
	mkdir "$PRIMARY_FILE" || fail "Unable to plant a directory at the metadata path."
	planning_run_backup_zxfer -k -P
	assertNotEquals "a directory at the metadata path must fail the run" 0 $?
	grep -Fq "Refusing to write backup metadata $PRIMARY_FILE because it is not a regular file." \
		"$CASE_DIR/zxfer.stderr" ||
		fail "expected the non-regular target refusal; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	assertTrue "the planted directory must stay" "[ -d '$PRIMARY_FILE' ]"
	assertEquals "no rename may run when the target is not a regular file" \
		0 "$(grep -c '^mv$' "$SPAWN_LOG")"

	# The alias target is checked before the previous primary is replaced.
	rmdir "$PRIMARY_FILE"
	printf 'old primary\n' >"$PRIMARY_FILE"
	chmod 600 "$PRIMARY_FILE"
	(umask 077 && mkdir -p "${FORWARDED_FILE%/*}") ||
		fail "Unable to create the alias directory."
	ln -s "$l_decoy" "$FORWARDED_FILE"
	planning_run_backup_zxfer -k -P
	assertNotEquals "a symlinked alias target must fail the run" 0 $?
	grep -Fq "Refusing to write backup metadata $FORWARDED_FILE because it is a symlink." \
		"$CASE_DIR/zxfer.stderr" ||
		fail "expected the symlinked alias refusal; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	assertEquals "the previous primary must be kept" "old primary" "$(cat "$PRIMARY_FILE")"
	assertTrue "the planted alias symlink must not be replaced" "[ -L '$FORWARDED_FILE' ]"
	assertEquals "the alias symlink target must be untouched" "decoy" "$(cat "$l_decoy")"
	assertEquals "no rename may run when the alias is refused" \
		0 "$(grep -c '^mv$' "$SPAWN_LOG")"
	assertEquals "a refused write leaves no stage file" \
		"" "$(find "$BACKUP_ROOT" -name '.zxfer-backup-*')"
}

# Invariant (-e fail-closed reads): a legacy flat-layout file is never
# consulted and an unsupported #format_version is rejected, both before any
# zfs argv is issued and with zero MUTATE lines.
test_restore_mode_rejects_legacy_layout_and_unsupported_format_version() {
	planning_setup_backup_env e_reject
	mkdir -p "$BACKUP_ROOT/$ZXFER_MOCKBIN_SOURCE_ROOT"
	printf '%s\n%s\n%s\n' "#zxfer property backup file" "#format_version:1" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT,$ZXFER_MOCKBIN_DEST_MAPPED_ROOT,compression=lz4" \
		>"$BACKUP_ROOT/$ZXFER_MOCKBIN_SOURCE_ROOT/.zxfer_backup_info.data"
	chmod 600 "$BACKUP_ROOT/$ZXFER_MOCKBIN_SOURCE_ROOT/.zxfer_backup_info.data"
	# Nor is the parent dataset's exact-pair file a candidate.
	l_ancestor_file=$(planning_backup_metadata_file "$BACKUP_ROOT" \
		"${ZXFER_MOCKBIN_SOURCE_ROOT%/*}" "$ZXFER_MOCKBIN_DEST_ROOT")
	(umask 077 && mkdir -p "${l_ancestor_file%/*}") ||
		fail "Unable to create the ancestor metadata directory."
	printf '%s\n' "#zxfer property backup file" "#format_version:2" \
		"#source_root:$ZXFER_MOCKBIN_SOURCE_ROOT" \
		"#destination_root:$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		".	compression=gzip=local" >"$l_ancestor_file"
	chmod 600 "$l_ancestor_file"

	planning_run_backup_zxfer -e
	l_run_status=$?
	assertEquals "-e with only a legacy layout and an ancestor's file must fail" 1 "$l_run_status"
	planning_assert_failure_report "backup metadata read" \
		"Cannot find backup property file. Ensure that it"
	grep -Fq "exists under the source-dataset-relative tree inside ZXFER_BACKUP_DIR." \
		"$CASE_DIR/zxfer.stderr" ||
		fail "the error must name ZXFER_BACKUP_DIR; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	grep -q '^usage:' "$CASE_DIR/zxfer.stderr" ||
		fail "a missing file is a usage error; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	assertEquals "the restore must fail before any zfs argv" "" "$(cat "$ZFS_LOG" 2>/dev/null)"

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "-k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	sed 's/^#format_version:2$/#format_version:999/' "$PRIMARY_FILE" >"$CASE_DIR/bad_version"
	cat "$CASE_DIR/bad_version" >"$PRIMARY_FILE"
	: >"$ZFS_LOG"

	planning_run_backup_zxfer -e
	assertNotEquals "-e with an unsupported format version must fail" 0 $?
	grep -q "does not declare supported zxfer backup metadata format version #format_version:2" \
		"$CASE_DIR/zxfer.stderr" ||
		fail "expected the unsupported-version error; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	assertEquals "the rejected restore must fail before any zfs argv" "" "$(cat "$ZFS_LOG" 2>/dev/null)"
}

# Invariant (-e restore): the source side reads the exact-pair file and the
# plan applies the RECORDED value (compression=lz4) even though the live
# source and destination both report gzip now.
test_restore_mode_applies_recorded_properties() {
	planning_setup_backup_env e_restore

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "-k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"

	l_drifted_rows=$(planning_property_rows_with "$(planning_property_default_rows)" \
		compression gzip local)
	planning_add_property_fixtures_for_rows "$l_drifted_rows" "$l_drifted_rows" \
		"$l_drifted_rows" "$l_drifted_rows"
	: >"$ZFS_LOG"

	planning_run_backup_zxfer -e
	l_run_status=$?
	assertEquals "-e must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	for l_restore_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"MUTATE set compression=lz4 $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_restore_suffix"
	done
	assertEquals "the restore sets exactly the recorded property on each dataset" \
		3 "$(grep -c '^MUTATE ' "$ZFS_LOG")"
	assertFalse "the drifted live value must never be applied" \
		"grep -q 'compression=gzip' '$ZFS_LOG'"
	planning_assert_no_send_receive
}

# Invariant (-T -k): with a remote destination both metadata files are
# published together through one ssh write script, and land
# with the same layout and 0600 mode (the mock ssh runs the rendered script
# locally through `sh -c`).
test_remote_target_backup_mode_writes_metadata_through_ssh() {
	planning_setup_backup_env k_remote
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_backup.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_backup_zxfer -T localhost -k -P
	l_remote_backup_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-T -k -P no-op must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_remote_backup_status"
	planning_assert_no_mutations
	planning_assert_no_send_receive
	planning_assert_backup_file_is_current_format "$PRIMARY_FILE" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	planning_assert_backup_file_is_current_format "$FORWARDED_FILE" \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "one remote write script publishes the metadata pair" \
		1 "$(planning_count_remote_script_marker '.zxfer-backup-write')"
}

# Invariant (-e retired name): a current-format file under the retired name
# .zxfer_backup_info.<tail>.k<cksum>.<length>, where cksum hashes
# "srcpool/data<LF>dstpool/back" with no final newline, still restores.
test_restore_mode_reads_a_current_file_under_the_retired_cksum_name() {
	planning_setup_backup_env e_retired
	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "-k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	mv "$PRIMARY_FILE" "$BACKUP_ROOT/$ZXFER_MOCKBIN_SOURCE_ROOT/.zxfer_backup_info.data.k1095302263.25" ||
		fail "Unable to rename the metadata file to its retired name."
	l_drifted_rows=$(planning_property_rows_with "$(planning_property_default_rows)" \
		compression gzip local)
	planning_add_property_fixtures_for_rows "$l_drifted_rows" "$l_drifted_rows" \
		"$l_drifted_rows" "$l_drifted_rows"
	: >"$ZFS_LOG"

	planning_run_backup_zxfer -e
	l_run_status=$?
	assertEquals "-e must restore from the retired name; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	for l_restore_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"MUTATE set compression=lz4 $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_restore_suffix"
	done
	assertFalse "restore never writes the current name" "[ -e '$PRIMARY_FILE' ]"
}

# Invariant (-k retired alias): a version-1 forwarded alias under the retired
# name (cksum of "srcpool/data<LF>srcpool/data") fails the chained -k run
# closed instead of silently recording live properties.
test_backup_mode_refuses_a_v1_forwarded_alias_under_the_retired_name() {
	planning_setup_backup_env k_retired_alias
	l_retired_alias="$BACKUP_ROOT/$ZXFER_MOCKBIN_SOURCE_ROOT/.zxfer_backup_info.data.k2770155462.25"
	(umask 077 && mkdir -p "${l_retired_alias%/*}") ||
		fail "Unable to create the retired alias directory."
	printf '%s\n' "#zxfer property backup file" "#format_version:1" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT,$ZXFER_MOCKBIN_SOURCE_ROOT,compression=gzip" \
		>"$l_retired_alias"
	chmod 600 "$l_retired_alias"

	planning_run_backup_zxfer -k -P
	assertNotEquals "a v1 alias must fail the -k run" 0 $?
	grep -q "Forwarded backup property file $l_retired_alias does not declare supported zxfer backup metadata format version #format_version:2." \
		"$CASE_DIR/zxfer.stderr" ||
		fail "expected the forwarded version refusal; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	assertFalse "no metadata may be written" "[ -e '$PRIMARY_FILE' ]"
	assertFalse "no alias may be written" "[ -e '$FORWARDED_FILE' ]"
}

# Invariant (-k chained provenance): an alias left by an earlier hop for a
# child dataset (keyed srcpool/data/child1 on both sides) supplies that
# child's row; datasets it does not cover keep their live properties.
test_backup_mode_forwards_an_alias_below_the_source_root() {
	planning_setup_backup_env k_child_alias
	l_child=$ZXFER_MOCKBIN_SOURCE_ROOT/child1
	l_child_alias=$(planning_backup_metadata_file "$BACKUP_ROOT" "$l_child" "$l_child")
	(umask 077 && mkdir -p "${l_child_alias%/*}") ||
		fail "Unable to create the child alias directory."
	printf '%s\n' "#zxfer property backup file" "#format_version:2" \
		"#source_root:$l_child" "#destination_root:$l_child" \
		".	compression=gzip=local" >"$l_child_alias"
	chmod 600 "$l_child_alias"

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "-k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertTrue "child1 records the forwarded provenance: $(cat "$PRIMARY_FILE")" \
		"grep -Fxq 'child1	compression=gzip=local' '$PRIMARY_FILE'"
	for l_live_key in . child2; do
		grep -q "^$l_live_key	.*compression=lz4=local" "$PRIMARY_FILE" ||
			fail "row $l_live_key must keep the live properties: $(cat "$PRIMARY_FILE")"
	done
}

# Invariant (-O -k chained provenance): over ssh the same child alias is
# found through ONE listing of the origin's storage directories, and only the
# roots that listing reports are read.
test_remote_origin_backup_mode_forwards_an_alias_below_the_source_root() {
	planning_setup_backup_env k_remote_child_alias
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	l_child=$ZXFER_MOCKBIN_SOURCE_ROOT/child1
	l_child_alias=$(planning_backup_metadata_file "$BACKUP_ROOT" "$l_child" "$l_child")
	(umask 077 && mkdir -p "${l_child_alias%/*}") ||
		fail "Unable to create the child alias directory."
	printf '%s\n' "#zxfer property backup file" "#format_version:2" \
		"#source_root:$l_child" "#destination_root:$l_child" \
		".	compression=gzip=local" >"$l_child_alias"
	chmod 600 "$l_child_alias"
	SSH_LOG="$CASE_DIR/ssh_backup_origin.log"
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"

	PATH="$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")" \
		planning_run_backup_zxfer -O localhost -k -P
	l_remote_origin_status=$?
	unset MOCK_SSH_LOG

	assertEquals "-O -k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_remote_origin_status"
	assertTrue "child1 records the forwarded provenance: $(cat "$PRIMARY_FILE")" \
		"grep -Fxq 'child1	compression=gzip=local' '$PRIMARY_FILE'"
	grep -q "^\.	.*compression=lz4=local" "$PRIMARY_FILE" ||
		fail "the root row must keep the live properties: $(cat "$PRIMARY_FILE")"
	assertEquals "one listing of the origin's storage directories" \
		1 "$(planning_count_remote_script_marker 'l_listing_dir')"
	# srcpool/data and srcpool are listed without an alias (current and
	# retired name each); child1's alias is found by its current name.
	assertEquals "each listed root is read once" \
		5 "$(planning_count_remote_script_marker 'l_expected_uid')"
}

# Purpose: Run a first -k -P hop from SOURCE_ROOT into DEST_ROOT that excludes
# its own source root with -x, so zxfer itself writes the forwarded alias of
# MAPPED_ROOT with child rows (compression=gzip) and no "." row. Restores the
# default fixture roots for the next hop; BACKUP_ROOT stays the case's.
# Usage: planning_run_rootless_backup_hop SOURCE_ROOT DEST_ROOT MAPPED_ROOT
planning_run_rootless_backup_hop() {
	planning_use_fixture_roots "$1" "$2" "$3"
	planning_setup_backup_env rootless_hop
	l_hop_rows=$(planning_property_default_rows)
	planning_add_property_fixtures_for_rows "$l_hop_rows" \
		"$(planning_property_rows_with "$l_hop_rows" compression gzip local)" \
		"$l_hop_rows" "$l_hop_rows"

	planning_run_backup_zxfer -k -P -x "^$1\$"
	l_run_status=$?
	assertEquals "hop 1 must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertTrue "hop 1 must write the forwarded alias of $3" "[ -f '$FORWARDED_FILE' ]"
	assertFalse "the alias must have no row for its own root: $(cat "$FORWARDED_FILE")" \
		"grep -q '^\.	' '$FORWARDED_FILE'"
	planning_use_fixture_roots "$planning_default_source_root" \
		"$planning_default_dest_root" "$planning_default_dest_mapped_root"
}

# Invariant (-k chained, root excluded below the source root): a hop that
# excluded its own root with -x leaves an alias without a "." row for
# srcpool/data/child1. A later -k of the parent reads it, finds no row for
# child1 there, and records every dataset. Regression: the run stopped at
# child1 with "does not contain a current-format relative row".
test_backup_mode_chains_through_an_alias_without_its_root_row_below_the_source_root() {
	planning_run_rootless_backup_hop qpool/child1 "$ZXFER_MOCKBIN_SOURCE_ROOT" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT/child1"
	planning_setup_backup_env rootless_below

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "the chained -k -P run must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	planning_assert_backup_file_is_current_format "$PRIMARY_FILE" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
}

# Invariant (-k chained, root excluded at the source root): the alias of
# srcpool/data itself has no "." row, so the root keeps its live properties
# while child1 and child2 record the forwarded gzip rows.
test_backup_mode_forwards_child_rows_from_an_alias_without_its_root_row() {
	planning_run_rootless_backup_hop qpool/data "${ZXFER_MOCKBIN_SOURCE_ROOT%/*}" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT"
	planning_setup_backup_env rootless_at_root

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "the chained -k -P run must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	grep -q "^\.	.*compression=lz4=local" "$PRIMARY_FILE" ||
		fail "the root row must keep the live properties: $(cat "$PRIMARY_FILE")"
	for l_forwarded_key in child1 child2; do
		grep -q "^$l_forwarded_key	.*compression=gzip=local" "$PRIMARY_FILE" ||
			fail "row $l_forwarded_key must record the forwarded properties: $(cat "$PRIMARY_FILE")"
	done
}

# Invariant (-n -k): a dry run previews only the backup-root preparation;
# with no property pass there is nothing to write, and nothing is created.
test_dry_run_backup_mode_previews_only_the_backup_root() {
	planning_setup_backup_env k_dry_run

	planning_run_backup_zxfer -n -v -k -P
	l_run_status=$?
	assertEquals "-n -k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertEquals "the preview is the root preparation plus the no-data note" \
		"Dry run: umask 077; 'mkdir' '-p' '$BACKUP_ROOT'; 'chmod' '700' '$BACKUP_ROOT'
No property data collected; skipping backup write." "$(cat "$CASE_DIR/zxfer.stdout")"
	assertEquals "a dry run issues no zfs argv" "" "$(cat "$ZFS_LOG" 2>/dev/null)"
	assertFalse "a dry run creates no backup root" "[ -e '$BACKUP_ROOT' ]"
}

# Invariant (-k write boundary): rows are published only at run end, so a
# run that fails at a later dataset leaves the previous complete files
# byte-identical and no stage file behind.
test_backup_mode_failure_partway_keeps_the_previous_files() {
	planning_setup_backup_env k_partial
	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "the first -k -P run must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	cp "$PRIMARY_FILE" "$CASE_DIR/primary.before"
	cp "$FORWARDED_FILE" "$CASE_DIR/forwarded.before"

	planning_clone_state "$FIXTURE_DIR/incremental" k_partial_incremental
	planning_add_property_transfer_fixtures
	{
		printf 'receive*child2*\t-\t1\n'
		cat "$STATE_DIR/manifest"
	} >"$STATE_DIR/manifest.new" ||
		fail "Unable to inject the child2 receive failure."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install the rewritten manifest."
	: >"$ZFS_LOG"

	planning_run_backup_zxfer -k -P
	assertNotEquals "the run must fail at child2" 0 $?
	planning_assert_log_has_line "END receive $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child1"
	assertTrue "the primary file is unchanged" "cmp -s '$CASE_DIR/primary.before' '$PRIMARY_FILE'"
	assertTrue "the forwarded alias is unchanged" "cmp -s '$CASE_DIR/forwarded.before' '$FORWARDED_FILE'"
	assertEquals "no stage file is left behind" \
		"" "$(find "$BACKUP_ROOT" -name '.zxfer-backup-*')"
}

# Invariant (-e file check): an exact-pair file that is not a complete
# current-format record of this pair fails the restore before any zfs argv,
# as a usage error naming the file and what is wrong with it.
test_restore_mode_rejects_invalid_exact_pair_files_before_any_zfs_call() {
	planning_setup_backup_env e_invalid
	(umask 077 && mkdir -p "${PRIMARY_FILE%/*}") ||
		fail "Unable to create the metadata directory."
	l_header="#zxfer property backup file"
	l_version="#format_version:2"
	l_roots="#source_root:$ZXFER_MOCKBIN_SOURCE_ROOT
#destination_root:$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	l_row=".	compression=lz4=local"
	# The error after "Backup property file <path> " | the file.
	for l_case in \
		"does not start with the required zxfer backup metadata header.|$l_version
$l_roots
$l_row" \
		"is malformed. Expected current-format relative-path and properties rows.|$l_header
$l_version
$l_roots
$l_row
$ZXFER_MOCKBIN_SOURCE_ROOT,$ZXFER_MOCKBIN_DEST_MAPPED_ROOT,compression=lz4" \
		"contains multiple relative rows for source dataset $ZXFER_MOCKBIN_SOURCE_ROOT.|$l_header
$l_version
$l_roots
$l_row
$l_row" \
		"does not contain a current-format relative row for source dataset $ZXFER_MOCKBIN_SOURCE_ROOT.|$l_header
$l_version
$l_roots
child1	compression=lz4=local" \
		"does not contain a current-format relative row for source dataset $ZXFER_MOCKBIN_SOURCE_ROOT.|$l_header
$l_version
#source_root:$ZXFER_MOCKBIN_SOURCE_ROOT
#destination_root:otherpool/back/data
$l_row"; do
		printf '%s\n' "${l_case#*|}" >"$PRIMARY_FILE"
		chmod 600 "$PRIMARY_FILE"
		: >"$ZFS_LOG"
		planning_run_backup_zxfer -e
		l_run_status=$?
		assertEquals "-e must refuse a file that [${l_case%%|*}]" 1 "$l_run_status"
		grep -Fq "Backup property file $PRIMARY_FILE ${l_case%%|*}" "$CASE_DIR/zxfer.stderr" ||
			fail "expected [${l_case%%|*}]; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
		grep -q '^usage:' "$CASE_DIR/zxfer.stderr" ||
			fail "an invalid file is a usage error; stderr: $(cat "$CASE_DIR/zxfer.stderr")"
		assertEquals "the refused restore issues no zfs argv" "" "$(cat "$ZFS_LOG" 2>/dev/null)"
	done
}

# Invariant (-O -e): the origin's read program returns the exact-pair file
# once and the plan sets the recorded values; without the file, the current
# and the retired name are both read and the restore stops before any zfs
# argv with the missing-file error.
test_remote_origin_restore_mode_reads_the_exact_pair_file_through_ssh() {
	planning_setup_backup_env e_remote_origin
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_restore.log"
	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "-k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	l_drifted_rows=$(planning_property_rows_with "$(planning_property_default_rows)" \
		compression gzip local)
	planning_add_property_fixtures_for_rows "$l_drifted_rows" "$l_drifted_rows" \
		"$l_drifted_rows" "$l_drifted_rows"
	: >"$ZFS_LOG"

	planning_run_backup_zxfer_over_ssh -O localhost -e
	l_run_status=$?
	assertEquals "-O -e must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	for l_restore_suffix in "" /child1 /child2; do
		planning_assert_log_has_line \
			"MUTATE set compression=lz4 $ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_restore_suffix"
	done
	assertEquals "the restore sets exactly the recorded property on each dataset" \
		3 "$(grep -c '^MUTATE ' "$ZFS_LOG")"
	assertEquals "the origin reads the exact-pair file once" \
		1 "$(planning_count_remote_script_marker 'l_expected_uid')"

	mv "$BACKUP_ROOT" "$CASE_DIR/backup_moved" || fail "Unable to move the backup root away."
	: >"$ZFS_LOG"
	planning_run_backup_zxfer_over_ssh -O localhost -e
	l_run_status=$?
	assertEquals "-O -e without the file must fail" 1 "$l_run_status"
	planning_assert_failure_report "backup metadata read" "Cannot find backup property file."
	assertEquals "the origin reads the current and the retired name" \
		2 "$(planning_count_remote_script_marker 'l_expected_uid')"
	assertEquals "the refused restore issues no zfs argv" "" "$(cat "$ZFS_LOG" 2>/dev/null)"
}

# Invariant (-n -T -k): a dry run prints the target's backup-directory
# program as one ssh command line, wrapper tokens included, runs nothing over
# ssh, and creates nothing.
test_remote_target_dry_run_backup_mode_previews_the_directory_program() {
	planning_setup_backup_env k_dry_run_remote
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	SSH_LOG="$CASE_DIR/ssh_dry_run.log"

	planning_run_backup_zxfer_over_ssh -n -v -T localhost -k -P
	l_run_status=$?
	assertEquals "-n -T -k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	assertEquals "the preview is one ssh line followed by the no-data note" \
		"No property data collected; skipping backup write." \
		"$(sed -n '2,$p' "$CASE_DIR/zxfer.stdout")"
	l_preview=$(sed -n 1p "$CASE_DIR/zxfer.stdout")
	for l_fragment in \
		"Dry run: '$MOCKBIN_DIR/ssh' " \
		" 'localhost' 'PATH='\''$MOCKBIN_DIR:" \
		"Refusing to use symlinked zxfer backup directory." \
		"mkdir -p '\''$BACKUP_ROOT'\''" \
		"chmod 700 '\''$BACKUP_ROOT'\''"; do
		assertContains "the preview must hold [$l_fragment]" "$l_preview" "$l_fragment"
	done
	assertEquals "a dry run runs nothing over ssh" "" "$(cat "$SSH_LOG")"

	planning_run_backup_zxfer_over_ssh -n -v -T 'localhost doas' -k -P
	l_run_status=$?
	assertEquals "-n -T 'localhost doas' -k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	assertEquals "the wrapped preview is still one ssh line" \
		2 "$(wc -l <"$CASE_DIR/zxfer.stdout" | tr -d ' ')"
	assertContains "the preview must keep the wrapper token" \
		"$(sed -n 1p "$CASE_DIR/zxfer.stdout")" "'localhost' ''\''doas'\''"
	assertEquals "a dry run runs nothing over ssh" "" "$(cat "$SSH_LOG")"
	assertEquals "a dry run issues no zfs argv" "" "$(cat "$ZFS_LOG" 2>/dev/null)"
	assertFalse "a dry run creates no backup root" "[ -e '$BACKUP_ROOT' ]"
}

# Invariant (ZXFER_BACKUP_DIR): a relative backup root, or one holding a TAB,
# CR or LF, stops the run at startup, before any zfs argv, and is never
# created.
test_backup_mode_refuses_unsafe_backup_dir_roots_at_startup() {
	planning_setup_backup_env k_unsafe_root
	l_tab=$(printf '\t')
	l_cr=$(printf '\r')
	# Run from CASE_DIR, so a relative root that got through lands there.
	l_launcher=$(cd "$(dirname "$ZXFER_TEST_ZXFER_BIN")" && pwd)/${ZXFER_TEST_ZXFER_BIN##*/}
	for l_root in relative/backup "$CASE_DIR/tab${l_tab}root" \
		"$CASE_DIR/cr${l_cr}root" "$CASE_DIR/lf
root"; do
		BACKUP_ROOT=$l_root
		: >"$ZFS_LOG"
		(
			ZXFER_MOCKBIN_ZXFER_BIN=$l_launcher
			cd "$CASE_DIR" && planning_run_backup_zxfer -k -P
		)
		l_run_status=$?
		assertEquals "the unsafe root [$l_root] must stop the run" 1 "$l_run_status"
		case $l_root in
		/*)
			l_reason="the backup metadata root must be a single-line absolute path without control whitespace."
			l_created=$l_root
			;;
		*)
			l_reason="because ZXFER_BACKUP_DIR must be an absolute path."
			l_created=$CASE_DIR/$l_root
			;;
		esac
		planning_assert_failure_report startup "$l_reason"
		assertEquals "the refused run issues no zfs argv" "" "$(cat "$ZFS_LOG" 2>/dev/null)"
		[ ! -e "$l_created" ] || fail "the refused root [$l_root] must not be created"
	done
}

# Invariant (inherited state): internal backup globals the caller exports
# neither move the backup root nor add rows: a dry run without
# ZXFER_BACKUP_DIR previews the default /var/db/zxfer, and a live run records
# only the datasets' own properties.
test_backup_mode_ignores_hostile_inherited_backup_state() {
	planning_setup_backup_env k_hostile_state
	l_decoy="$CASE_DIR/decoy_root"
	l_tab=$(printf '\t')
	(
		g_backup_storage_root=$l_decoy
		g_backup_file_contents="child9${l_tab}compression=forged=local"
		g_zxfer_backup_forwarded_roots="$ZXFER_MOCKBIN_SOURCE_ROOT/child1${l_tab}$l_decoy/alias"
		g_zxfer_backup_forwarded_rows="$ZXFER_MOCKBIN_SOURCE_ROOT/child1${l_tab}$ZXFER_MOCKBIN_SOURCE_ROOT/child1${l_tab}compression=forged=local"
		export g_backup_storage_root g_backup_file_contents \
			g_zxfer_backup_forwarded_roots g_zxfer_backup_forwarded_rows
		planning_run_zxfer "$STATE_DIR" -n -v -k -P -R \
			"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
		printf '%s\n' "$?" >"$CASE_DIR/dry_run.status"
		cp "$CASE_DIR/zxfer.stdout" "$CASE_DIR/dry_run.stdout"
		planning_run_backup_zxfer -k -P
	)
	l_run_status=$?
	assertEquals "the dry run must exit 0" 0 "$(cat "$CASE_DIR/dry_run.status")"
	assertEquals "without ZXFER_BACKUP_DIR the dry run previews the default root" \
		"Dry run: umask 077; 'mkdir' '-p' '/var/db/zxfer'; 'chmod' '700' '/var/db/zxfer'" \
		"$(sed -n 1p "$CASE_DIR/dry_run.stdout")"
	assertEquals "-k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" 0 "$l_run_status"
	planning_assert_backup_file_is_current_format "$PRIMARY_FILE" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertFalse "no inherited row may reach the file: $(cat "$PRIMARY_FILE")" \
		"grep -q forged '$PRIMARY_FILE'"
	assertFalse "the inherited root must never be used" "[ -e '$l_decoy' ]"
}

# Invariant (-k ambiguous provenance): a forwarded alias with two rows for
# child1 fails the -k run closed at child1, naming the alias, and writes no
# metadata; a run without -k never reads the backup store.
test_backup_mode_fails_closed_on_an_alias_with_duplicate_rows() {
	planning_setup_backup_env k_duplicate_alias
	planning_write_backup_alias "$ZXFER_MOCKBIN_SOURCE_ROOT" ".	compression=gzip=local" \
		"child1	compression=off=local" "child1	compression=zstd=local"
	l_alias=$(planning_backup_metadata_file "$BACKUP_ROOT" "$ZXFER_MOCKBIN_SOURCE_ROOT" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT")

	planning_run_backup_zxfer -P
	l_run_status=$?
	assertEquals "-P without -k must not read the store; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "the ambiguous alias must fail the -k run" 1 "$l_run_status"
	planning_assert_failure_report "property transfer" \
		"Forwarded backup property file $l_alias contains multiple relative rows for source dataset $ZXFER_MOCKBIN_SOURCE_ROOT/child1."
	planning_assert_no_mutations
	assertFalse "no metadata may be written" "[ -e '$PRIMARY_FILE' ]"
	assertFalse "no alias may be written" "[ -e '$FORWARDED_FILE' ]"
}

# Invariant (-k chained provenance, two hops): an -R hop left the pool alias
# of srcpool (rows data and data/child2) and a -N hop the alias of
# srcpool/data ("." only). The nearest alias with a row wins: srcpool/data
# records the -N hop's row, child2 walks past srcpool/data to the pool alias,
# and child1, in neither, keeps its live properties. Over -O one listing
# finds both aliases and each is read once.
test_backup_mode_forwards_the_nearest_alias_row_across_two_aliases() {
	planning_setup_backup_env k_two_aliases
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	planning_write_backup_alias "${ZXFER_MOCKBIN_SOURCE_ROOT%/*}" ".	compression=off=local" \
		"data	compression=zstd=local" "data/child2	compression=zstd-3=local"
	planning_write_backup_alias "$ZXFER_MOCKBIN_SOURCE_ROOT" ".	compression=gzip=local"
	SSH_LOG="$CASE_DIR/ssh_two_aliases.log"

	for l_mode in local origin; do
		rm -f "$PRIMARY_FILE" "$FORWARDED_FILE"
		if [ "$l_mode" = local ]; then
			planning_run_backup_zxfer -k -P
		else
			planning_run_backup_zxfer_over_ssh -O localhost -k -P
		fi
		l_run_status=$?
		assertEquals "$l_mode -k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
			0 "$l_run_status"
		assertTrue "$l_mode: the root records the -N hop's row: $(cat "$PRIMARY_FILE")" \
			"grep -Fxq '.	compression=gzip=local' '$PRIMARY_FILE'"
		assertTrue "$l_mode: child2 records the pool alias's row: $(cat "$PRIMARY_FILE")" \
			"grep -Fxq 'child2	compression=zstd-3=local' '$PRIMARY_FILE'"
		grep -q "^child1	.*compression=lz4=local" "$PRIMARY_FILE" ||
			fail "$l_mode: child1 must keep the live properties: $(cat "$PRIMARY_FILE")"
	done
	assertEquals "one listing of the origin's storage directories" \
		1 "$(planning_count_remote_script_marker 'l_listing_dir')"
	assertEquals "each alias is read once" \
		2 "$(planning_count_remote_script_marker 'l_expected_uid')"
}

# Invariant (-k post-seed checkpoint): a missing child is created and seeded,
# so its row is captured before the seed and again by the post-seed
# reconcile. The pair is published at that checkpoint and at run end, and
# each file still holds one row per dataset.
test_backup_mode_records_one_row_per_dataset_after_a_seeded_reconcile() {
	planning_setup_backup_env k_seeded
	l_missing_child="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child2"
	for l_seed_fixture in dst_datasets.list dst_snapshots.list dst_props_tree.list; do
		grep -v "^$l_missing_child" "$STATE_DIR/$l_seed_fixture" \
			>"$STATE_DIR/$l_seed_fixture.new" || :
		mv "$STATE_DIR/$l_seed_fixture.new" "$STATE_DIR/$l_seed_fixture" ||
			fail "Unable to drop $l_missing_child from $l_seed_fixture."
	done
	: >"$STATE_DIR/dst_d1_2.list"
	printf "cannot open '%s': dataset does not exist\n" "$l_missing_child" \
		>"$STATE_DIR/missing_child2.list"
	printf 'list -H %s\tmissing_child2.list\t1\n' "$l_missing_child" \
		>>"$STATE_DIR/manifest" || fail "Unable to append missing-child rule."

	planning_run_backup_zxfer -k -P
	l_run_status=$?
	assertEquals "-k -P with a seeded child must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	planning_assert_log_has_line "receive -F $l_missing_child"
	planning_assert_backup_file_is_current_format "$PRIMARY_FILE" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	planning_assert_backup_file_is_current_format "$FORWARDED_FILE" \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	assertEquals "the post-seed checkpoint and the run end each publish the pair" \
		4 "$(grep -c '^mv$' "$SPAWN_LOG")"
}

# Invariant (-k literal payload): a property value holding $(...), backticks,
# backslashes and a quote is recorded verbatim in both files, locally and
# over -T, and none of it runs.
test_backup_mode_records_hostile_property_values_literally() {
	planning_setup_backup_env k_hostile_values
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	l_sentinel="$CASE_DIR/value_ran"
	l_value="\$(touch $l_sentinel)\`touch $l_sentinel\`\\\\t'q"
	l_rows=$(printf '%s\nuser:note\t%s\tlocal' "$(planning_property_default_rows)" "$l_value")
	planning_add_property_fixtures_for_rows "$l_rows" "$l_rows" "$l_rows" "$l_rows"
	SSH_LOG="$CASE_DIR/ssh_hostile.log"

	for l_mode in local remote; do
		rm -rf "$BACKUP_ROOT"
		if [ "$l_mode" = local ]; then
			planning_run_backup_zxfer -k -P
		else
			planning_run_backup_zxfer_over_ssh -T localhost -k -P
		fi
		l_run_status=$?
		assertEquals "$l_mode -k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
			0 "$l_run_status"
		for l_file in "$PRIMARY_FILE" "$FORWARDED_FILE"; do
			grep -Fq "user:note=$l_value=local" "$l_file" ||
				fail "$l_mode: the value must be recorded verbatim in $l_file: $(cat "$l_file")"
		done
		assertFalse "$l_mode: the value must never run" "[ -e '$l_sentinel' ]"
	done
}

# Invariant (-T wrapper spec): with -T 'localhost pfexec' the backup-directory
# and pair write programs run through the wrapper like every other remote
# command (a pass-through pfexec stands in for the real one).
test_remote_target_backup_mode_keeps_a_wrapper_host_spec() {
	planning_setup_backup_env k_remote_wrapper
	planning_write_socket_mock_ssh "$MOCKBIN_DIR/ssh" ||
		fail "Unable to write socket-aware mock ssh."
	cat >"$MOCKBIN_DIR/pfexec" <<'EOF'
#!/bin/sh
exec "$@"
EOF
	chmod +x "$MOCKBIN_DIR/pfexec"
	SSH_LOG="$CASE_DIR/ssh_wrapper.log"

	planning_run_backup_zxfer_over_ssh -T 'localhost pfexec' -k -P
	l_run_status=$?
	assertEquals "-T 'localhost pfexec' -k -P must exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
	planning_assert_backup_file_is_current_format "$PRIMARY_FILE" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	planning_assert_backup_file_is_current_format "$FORWARDED_FILE" \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
	grep -F "localhost 'pfexec' " "$SSH_LOG" >"$CASE_DIR/ssh_wrapped.log" || :
	grep -vF "localhost 'pfexec' " "$SSH_LOG" >"$CASE_DIR/ssh_unwrapped.log" || :
	SSH_LOG="$CASE_DIR/ssh_wrapped.log"
	assertEquals "the pair write program runs through the wrapper" \
		1 "$(planning_count_remote_script_marker '.zxfer-backup-write')"
	SSH_LOG="$CASE_DIR/ssh_unwrapped.log"
	assertEquals "no backup program bypasses the wrapper" \
		0 "$(planning_count_remote_script_marker 'Backup path exists but is not a directory.')"
}

. "$SHUNIT2_BIN"
