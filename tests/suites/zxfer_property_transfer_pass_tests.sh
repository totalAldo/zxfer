#!/bin/sh
# Property transfer fragment: zxfer_transfer_properties driven end to end
# against a fake zfs (reads answered by role, mutations logged). Run by
# tests/test_zxfer_property_transfer.sh.
# shellcheck disable=SC1090,SC2030,SC2031,SC2034,SC2154,SC2317,SC2329

# Fake `zfs get` answers keyed on role and dataset. Datasets and their rows
# (zfs get -H tab form) are declared per test through the TRANSFER_* variables;
# the machine and human views share the same rows unless a *_HUMAN variant is
# set, and the `-o property` skeleton lists the rows' names. Unknown shapes
# fail closed so a test cannot silently pass on a probe it did not model.
zxfer_property_test_answer_read() {
	case "$1 $2 $5 $6" in
	"source get all tank/src")
		l_answer_rows=$TRANSFER_SRC_ROWS
		[ "$3" != -Ho ] || l_answer_rows=${TRANSFER_SRC_ROWS_HUMAN:-$l_answer_rows}
		;;
	"source get all tank/src/child") l_answer_rows=$TRANSFER_SRC_CHILD_ROWS ;;
	"destination get all backup/dst") l_answer_rows=$TRANSFER_DST_ROWS ;;
	"destination get all backup/dst/child") l_answer_rows=$TRANSFER_DST_CHILD_ROWS ;;
	"source get "*" tank/src")
		[ "$3 $4" = "-Hpo property,value,source" ] &&
			[ -n "${TRANSFER_SRC_PROBE_ROWS:-}" ] || return 1
		printf '%b' "$TRANSFER_SRC_PROBE_ROWS"
		return 0
		;;
	*) l_answer_rows="" ;;
	esac
	case "$3 $4" in
	"-Ho property") [ -z "$l_answer_rows" ] || printf '%b' "$l_answer_rows" | cut -f1 ;;
	"-Hpo property,value,source" | "-Ho property,value,source") printf '%b' "$l_answer_rows" ;;
	*) l_answer_rows="" ;;
	esac
	[ -z "$l_answer_rows" ] || return 0
	printf 'unexpected zfs read: %s\n' "$*" >&2
	return 1
}

# Log one read to TRANSFER_READ_LOG, then answer it.
zxfer_property_test_transfer_reads() {
	printf '%s\n' "$*" >>"$TRANSFER_READ_LOG"
	zxfer_property_test_answer_read "$@"
}

# Failure-injecting read fakes, defined at top level because a case statement
# inside "$(...)" is not portable across shells.
zxfer_property_test_reads_destination_fails() {
	[ "$1" != destination ] || return 4
	zxfer_property_test_answer_read "$@"
}

zxfer_property_test_reads_required_probe_fails() {
	case "$*" in
	*" all tank/src") zxfer_property_test_answer_read "$@" ;;
	*)
		printf 'permission denied\n' >&2
		return 5
		;;
	esac
}

zxfer_property_test_reads_parent_fails() {
	printf '%s\n' "$*" >>"$TRANSFER_READ_LOG"
	case "$*" in
	"destination get "*" all backup/dst") return 6 ;;
	esac
	zxfer_property_test_answer_read "$@"
}

zxfer_property_test_transfer_mutations() {
	printf 'MUTATE %s\n' "$*" >>"$TRANSFER_MUTATE_LOG"
	[ "${TRANSFER_MUTATE_STATUS:-0}" -eq 0 ] || return "$TRANSFER_MUTATE_STATUS"
}

# Run zxfer_transfer_properties in a subshell with the fakes installed and
# print the mutation log; stdout of the transfer itself is captured too.
zxfer_property_test_run_transfer() {
	: >"$TRANSFER_READ_LOG"
	: >"$TRANSFER_MUTATE_LOG"
	(
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=${TRANSFER_DEST_EXISTS:-1}
		}
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_transfer_reads "$@"; }
		zxfer_run_destination_zfs_cmd() { zxfer_property_test_transfer_mutations "$@"; }
		zxfer_throw_error() {
			printf 'THROW %s|%s\n' "$1" "${2:-1}"
			exit "${2:-1}"
		}
		zxfer_throw_usage_error() {
			printf 'USAGE %s\n' "$1"
			exit "${2:-2}"
		}
		zxfer_throw_error_with_usage() {
			printf 'USAGE %s\n' "$1"
			exit "${2:-2}"
		}
		zxfer_transfer_properties "$@"
	)
}

zxfer_property_test_default_rows() {
	TRANSFER_SRC_ROWS='type\tfilesystem\t-\ncompression\tlz4\tlocal\natime\toff\tlocal\nreadonly\toff\tlocal\nmountpoint\t/mnt\tlocal\ncasesensitivity\tsensitive\t-\nnormalization\tnone\t-\nutf8only\toff\t-\n'
	TRANSFER_DST_ROWS=$TRANSFER_SRC_ROWS
	TRANSFER_SRC_CHILD_ROWS='type\tfilesystem\t-\ncompression\tlz4\tinherited from tank/src\natime\toff\tlocal\ncasesensitivity\tsensitive\t-\nnormalization\tnone\t-\nutf8only\toff\t-\n'
	TRANSFER_DST_CHILD_ROWS=$TRANSFER_SRC_CHILD_ROWS
	TRANSFER_SRC_ROWS_HUMAN=""
	TRANSFER_SRC_PROBE_ROWS=""
	TRANSFER_MUTATE_STATUS=0
	TRANSFER_DEST_EXISTS=1
	TRANSFER_READ_LOG="$TEST_TMPDIR/transfer_reads.log"
	TRANSFER_MUTATE_LOG="$TEST_TMPDIR/transfer_mutations.log"
	# The suite trims the readonly list; keep the entries every transfer
	# fixture here relies on (type and volsize are read-only in production).
	ZXFER_BASE_READONLY_PROPERTIES="type,readonly,mountpoint,volsize"
	g_option_P_transfer_property=1
	g_initial_source="tank/src"
	g_actual_dest="backup/dst"
	g_recursive_dest_list="backup/dst
backup/dst/child"
}

test_transfer_properties_is_a_noop_for_identical_datasets() {
	zxfer_property_test_default_rows
	output=$(zxfer_property_test_run_transfer "tank/src")
	assertEquals 0 "$?"
	assertEquals "" "$output"
	assertEquals "" "$(cat "$TRANSFER_MUTATE_LOG")"
}

test_transfer_properties_diffs_existing_destination_and_sets_differing_local_properties() {
	zxfer_property_test_default_rows
	TRANSFER_DST_ROWS='type\tfilesystem\t-\ncompression\tgzip\tlocal\natime\ton\tlocal\nreadonly\toff\tlocal\nmountpoint\t/elsewhere\tlocal\ncasesensitivity\tsensitive\t-\nnormalization\tnone\t-\nutf8only\toff\t-\n'
	zxfer_property_test_run_transfer "tank/src"
	assertEquals 0 "$?"
	assertEquals "Differing settable properties are set in one batch; read-only mountpoint is never set." \
		"MUTATE set compression=lz4 atime=off backup/dst" "$(cat "$TRANSFER_MUTATE_LOG")"
}

test_transfer_properties_creates_missing_destination_with_creation_properties_and_records_backup() {
	zxfer_property_test_default_rows
	g_recursive_dest_list=""
	g_option_k_backup_property_mode=1
	output=$(
		TRANSFER_DEST_EXISTS=0
		zxfer_append_backup_metadata_record() { printf 'backup_append %s %s\n' "$1" "$2"; }
		zxfer_property_test_run_transfer "tank/src"
	)
	assertEquals 0 "$?"
	assertEquals "Local and creation-time properties are created; readonly-listed ones (readonly, mountpoint here) are not." \
		"MUTATE create -p backup
MUTATE create -o compression=lz4 -o atime=off -o casesensitivity=sensitive -o normalization=none -o utf8only=off backup/dst" \
		"$(cat "$TRANSFER_MUTATE_LOG")"
	assertEquals "backup_append tank/src type=filesystem=-,compression=lz4=local,atime=off=local,readonly=off=local,mountpoint=/mnt=local,casesensitivity=sensitive=-,normalization=none=-,utf8only=off=-" \
		"$output"
	assertEquals "A created destination is never read back for a diff." \
		0 "$(grep -c '^destination' "$TRANSFER_READ_LOG")"
}

test_transfer_properties_applies_override_value_on_the_initial_source() {
	zxfer_property_test_default_rows
	g_option_o_override_property="compression=gzip"
	zxfer_property_test_run_transfer "tank/src"
	assertEquals 0 "$?"
	assertEquals "MUTATE set compression=gzip backup/dst" "$(cat "$TRANSFER_MUTATE_LOG")"
}

test_transfer_properties_rejects_overrides_missing_from_the_source() {
	zxfer_property_test_default_rows
	g_option_o_override_property="copies=2"
	output=$(zxfer_property_test_run_transfer "tank/src")
	assertEquals 2 "$?"
	assertEquals "USAGE Missing source property for -o override: copies." "$output"
	assertEquals "" "$(cat "$TRANSFER_MUTATE_LOG")"
}

test_transfer_properties_checks_override_names_on_the_initial_source_only() {
	zxfer_property_test_default_rows
	g_option_o_override_property="copies=2"
	g_actual_dest="backup/dst/child"
	output=$(zxfer_property_test_run_transfer "tank/src/child")
	assertEquals "A child derives -o without re-checking it against its own source." 0 "$?"
	assertEquals "" "$output"
	assertEquals "With -P, an override the child source lacks is not applied." \
		"" "$(cat "$TRANSFER_MUTATE_LOG")"
}

test_transfer_properties_checks_override_names_before_probing_the_destination() {
	zxfer_property_test_default_rows
	g_recursive_dest_list=""
	g_option_o_override_property="copies=2"
	: >"$TRANSFER_READ_LOG"
	: >"$TRANSFER_MUTATE_LOG"
	output=$(
		(
			zxfer_probe_destination_existence() {
				printf 'probe %s\n' "$1" >>"$TRANSFER_READ_LOG"
				g_zxfer_destination_exists_result=0
			}
			zxfer_run_zfs_cmd_for_role() { zxfer_property_test_transfer_reads "$@"; }
			zxfer_run_destination_zfs_cmd() { zxfer_property_test_transfer_mutations "$@"; }
			zxfer_throw_usage_error() {
				printf 'USAGE %s\n' "$1"
				exit 2
			}
			zxfer_transfer_properties "tank/src"
		)
	)
	assertEquals 2 "$?"
	assertEquals "USAGE Missing source property for -o override: copies." "$output"
	assertEquals "Only the source is read; the unlisted destination is never probed." \
		"" "$(grep -v '^source ' "$TRANSFER_READ_LOG")"
	assertEquals "" "$(cat "$TRANSFER_MUTATE_LOG")"
}

test_transfer_properties_preserves_escaped_comma_override_end_to_end() {
	zxfer_property_test_default_rows
	TRANSFER_SRC_ROWS='type\tfilesystem\t-\nuser:note\told\tlocal\ncasesensitivity\tsensitive\t-\nnormalization\tnone\t-\nutf8only\toff\t-\n'
	TRANSFER_DST_ROWS=$TRANSFER_SRC_ROWS
	g_option_o_override_property='user:note=a\,b=c'
	(
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=${TRANSFER_DEST_EXISTS:-1}
		}
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_transfer_reads "$@"; }
		zxfer_run_destination_zfs_cmd() {
			printf '%s\n' "$#"
			printf '%s\n' "$@"
		}
		TRANSFER_READ_LOG="$TEST_TMPDIR/transfer_reads.log"
		zxfer_transfer_properties "tank/src"
	) >"$TEST_TMPDIR/transfer_escaped.out"
	assertEquals "$(printf '3\nset\nuser:note=a,b=c\nbackup/dst')" "$(cat "$TEST_TMPDIR/transfer_escaped.out")"
}

test_transfer_properties_skips_ignored_properties() {
	zxfer_property_test_default_rows
	TRANSFER_DST_ROWS='type\tfilesystem\t-\ncompression\tgzip\tlocal\natime\ton\tlocal\nreadonly\toff\tlocal\nmountpoint\t/mnt\tlocal\ncasesensitivity\tsensitive\t-\nnormalization\tnone\t-\nutf8only\toff\t-\n'
	g_option_I_ignore_properties="compression"
	zxfer_property_test_run_transfer "tank/src"
	assertEquals "MUTATE set atime=off backup/dst" "$(cat "$TRANSFER_MUTATE_LOG")"
}

test_transfer_properties_strips_unsupported_properties_for_the_current_dataset_type() {
	zxfer_property_test_default_rows
	TRANSFER_DST_ROWS='type\tfilesystem\t-\natime\toff\tlocal\nreadonly\toff\tlocal\nmountpoint\t/mnt\tlocal\ncasesensitivity\tsensitive\t-\nnormalization\tnone\t-\nutf8only\toff\t-\n'
	g_option_U_skip_unsupported_properties=1
	g_zxfer_unsupported_filesystem_properties="compression"
	g_zxfer_unsupported_volume_properties=""
	zxfer_property_test_run_transfer "tank/src"
	assertEquals 0 "$?"
	assertEquals "The filesystem list applies to a filesystem source." "" "$(cat "$TRANSFER_MUTATE_LOG")"

	g_zxfer_unsupported_filesystem_properties=""
	g_zxfer_unsupported_volume_properties="compression"
	zxfer_property_test_run_transfer "tank/src"
	assertEquals "The volume list does not apply to a filesystem source." \
		"MUTATE set compression=lz4 backup/dst" "$(cat "$TRANSFER_MUTATE_LOG")"
}

test_transfer_properties_backfills_required_create_props_and_enforces_must_create_rules() {
	zxfer_property_test_default_rows
	TRANSFER_SRC_ROWS='type\tfilesystem\t-\ncompression\tlz4\tlocal\n'
	TRANSFER_SRC_PROBE_ROWS='casesensitivity\tinsensitive\t-\nnormalization\tnone\t-\nutf8only\toff\t-\n'
	output=$(zxfer_property_test_run_transfer "tank/src")
	assertEquals 2 "$?"
	assertContains "$output" "USAGE The property \"casesensitivity\" may only be set"
	assertEquals "" "$(cat "$TRANSFER_MUTATE_LOG")"
	assertContains "Missing creation-time properties are probed with one comma-list call." \
		"$(cat "$TRANSFER_READ_LOG")" "source get -Hpo property,value,source casesensitivity,normalization,utf8only tank/src"
}

test_transfer_properties_creates_volumes_with_size_and_skips_filesystem_only_probes() {
	zxfer_property_test_default_rows
	TRANSFER_SRC_ROWS='type\tvolume\t-\nvolsize\t1073741824\tlocal\ncompression\tlz4\tlocal\nrefreservation\t1073741824\treceived\n'
	g_recursive_dest_list=""
	TRANSFER_DEST_EXISTS=0
	zxfer_property_test_run_transfer "tank/src"
	assertEquals 0 "$?"
	assertEquals "MUTATE create -p backup
MUTATE create -V 1073741824 -o compression=lz4 -o refreservation=1073741824 backup/dst" \
		"$(cat "$TRANSFER_MUTATE_LOG")"
	assertEquals "Volumes never probe creation-time filesystem properties or a separate type/volsize." \
		"source get -Hpo property,value,source all tank/src
source get -Ho property,value,source all tank/src
source get -Ho property all tank/src" "$(cat "$TRANSFER_READ_LOG")"
}

test_transfer_properties_fails_when_source_property_read_fails() {
	zxfer_property_test_default_rows
	output=$(
		zxfer_property_test_transfer_reads() {
			printf 'cannot open tank/src: permission denied\n' >&2
			return 3
		}
		zxfer_property_test_run_transfer "tank/src"
	)
	assertEquals 3 "$?"
	assertEquals "THROW cannot open tank/src: permission denied|3" "$output"

	output=$(
		zxfer_property_test_transfer_reads() { return 1; }
		zxfer_property_test_run_transfer "tank/src"
	)
	assertEquals 1 "$?"
	assertEquals "THROW Failed to retrieve source properties for [tank/src].|1" "$output"
}

test_transfer_properties_fails_on_invalid_source_type_and_empty_volume_size() {
	zxfer_property_test_default_rows
	TRANSFER_SRC_ROWS='type\tsnapshot\t-\ncompression\tlz4\tlocal\n'
	output=$(zxfer_property_test_run_transfer "tank/src")
	assertEquals "THROW Invalid source dataset type for [tank/src]: snapshot|1" "$output"

	TRANSFER_SRC_ROWS='type\tvolume\t-\nvolsize\t-\t-\n'
	output=$(zxfer_property_test_run_transfer "tank/src")
	assertEquals "THROW Failed to retrieve source zvol size for [tank/src]: empty volsize|1" "$output"
}

test_transfer_properties_fails_when_destination_property_read_fails() {
	zxfer_property_test_default_rows
	output=$(
		zxfer_property_test_transfer_reads() { zxfer_property_test_reads_destination_fails "$@"; }
		zxfer_property_test_run_transfer "tank/src"
	)
	assertEquals 4 "$?"
	assertEquals "THROW Failed to retrieve destination properties for [backup/dst].|4" "$output"

	TRANSFER_DST_ROWS='type\tfilesystem\t-\ncompression\tlz4\tlocal\ncompression\tgzip\tlocal\n'
	output=$(zxfer_property_test_run_transfer "tank/src")
	assertEquals "A repeated destination property name fails closed with the parse diagnostic." \
		"THROW Failed to retrieve destination properties for [backup/dst]: Failed to parse the properties of dataset [backup/dst]: the zfs get property list is malformed, repeats a name, or does not match the values.|1" "$output"
	assertEquals "" "$(cat "$TRANSFER_MUTATE_LOG")"
}

test_transfer_properties_fails_when_required_property_probe_fails() {
	zxfer_property_test_default_rows
	TRANSFER_SRC_ROWS='type\tfilesystem\t-\ncompression\tlz4\tlocal\n'
	output=$(
		zxfer_property_test_transfer_reads() { zxfer_property_test_reads_required_probe_fails "$@"; }
		zxfer_property_test_run_transfer "tank/src"
	)
	assertEquals 5 "$?"
	assertEquals "THROW Failed to retrieve required creation-time property [casesensitivity] for dataset [tank/src]: permission denied|5" "$output"
}

test_transfer_properties_adjusts_child_inherit_lists_against_the_destination_parent() {
	zxfer_property_test_default_rows
	TRANSFER_DST_CHILD_ROWS='type\tfilesystem\t-\ncompression\tlz4\tlocal\natime\ton\tlocal\ncasesensitivity\tsensitive\t-\nnormalization\tnone\t-\nutf8only\toff\t-\n'
	g_actual_dest="backup/dst/child"
	zxfer_property_test_run_transfer "tank/src/child"
	assertEquals 0 "$?"
	assertEquals "A local destination copy of an inherited source value is inherited from the matching parent; local source values are set." \
		"MUTATE set atime=off backup/dst/child
MUTATE inherit compression backup/dst/child" "$(cat "$TRANSFER_MUTATE_LOG")"
	assertContains "The parent is read from the destination side for the inherit adjustment." \
		"$(cat "$TRANSFER_READ_LOG")" "destination get -Hpo property,value,source all backup/dst"
}

test_transfer_properties_promotes_child_inherits_to_sets_when_the_parent_differs() {
	zxfer_property_test_default_rows
	TRANSFER_DST_ROWS='type\tfilesystem\t-\ncompression\tgzip\tlocal\natime\toff\tlocal\nreadonly\toff\tlocal\nmountpoint\t/mnt\tlocal\ncasesensitivity\tsensitive\t-\nnormalization\tnone\t-\nutf8only\toff\t-\n'
	TRANSFER_DST_CHILD_ROWS='type\tfilesystem\t-\ncompression\tlz4\tlocal\natime\toff\tlocal\ncasesensitivity\tsensitive\t-\nnormalization\tnone\t-\nutf8only\toff\t-\n'
	g_actual_dest="backup/dst/child"
	zxfer_property_test_run_transfer "tank/src/child"
	assertEquals "MUTATE set compression=lz4 backup/dst/child" "$(cat "$TRANSFER_MUTATE_LOG")"
}

test_transfer_properties_escapes_control_bytes_in_very_verbose_list_dumps() {
	zxfer_property_test_default_rows
	# printf %b turns \033, \007 and \r into raw bytes and \\ into one backslash.
	TRANSFER_SRC_CHILD_ROWS=$TRANSFER_SRC_CHILD_ROWS'com.x:note\tok\033[2J\033]0;x\007 lit=\\033 \\c cut\r\tlocal\n'
	g_actual_dest="backup/dst/child"
	(
		g_option_V_very_verbose=1
		zxfer_property_test_run_transfer "tank/src/child"
	) 2>"$TEST_TMPDIR/transfer_dumps.err"
	assertEquals 0 "$?"
	l_dumps=$(cat "$TEST_TMPDIR/transfer_dumps.err")
	assertEquals "-V list dumps hold no control byte but LF." \
		0 "$(zxfer_property_test_count_control_bytes "$l_dumps")"
	# assertContains pipes through echo, which expands backslashes on some
	# shells, so the escaped text is matched with case.
	l_shown='com.x:note=ok\x1B[2J\x1B]0%3Bx\x07 lit%3D\\033 \\c cut%0D'
	for l_dump in "override_pvs" "creation_pvs" "init_set" "child_set" "adjusted child_set"; do
		l_line=$(printf '%s\n' "$l_dumps" | grep "^zxfer_transfer_properties $l_dump: ")
		case $l_line in
		*"$l_shown"*) ;;
		*) fail "The $l_dump dump should show the value escaped: $l_line" ;;
		esac
	done
	assertEquals "zfs still gets the raw value." \
		"$(printf 'MUTATE set com.x:note=ok\033[2J\033]0;x\007 lit=\\033 \\c cut\r backup/dst/child')" \
		"$(cat "$TRANSFER_MUTATE_LOG")"
}

test_transfer_properties_reports_adjust_child_inherit_failures() {
	zxfer_property_test_default_rows
	TRANSFER_DST_CHILD_ROWS='type\tfilesystem\t-\ncompression\tlz4\tlocal\natime\toff\tlocal\ncasesensitivity\tsensitive\t-\nnormalization\tnone\t-\nutf8only\toff\t-\n'
	g_actual_dest="backup/dst/child"
	output=$(
		zxfer_property_test_transfer_reads() { zxfer_property_test_reads_parent_fails "$@"; }
		zxfer_property_test_run_transfer "tank/src/child"
	)
	assertEquals 6 "$?"
	assertEquals "THROW Failed to reconcile inherited child properties for destination [backup/dst/child].|6" "$output"
}

test_transfer_properties_uses_platform_readonly_lists_without_mutating_global_state() {
	zxfer_property_test_default_rows
	TRANSFER_SRC_ROWS='type\tfilesystem\t-\naclmode\tpassthrough\tlocal\ncompression\tlz4\tlocal\ncasesensitivity\tsensitive\t-\nnormalization\tnone\t-\nutf8only\toff\t-\n'
	TRANSFER_DST_ROWS='type\tfilesystem\t-\naclmode\tdiscard\tlocal\ncompression\toff\tlocal\ncasesensitivity\tsensitive\t-\nnormalization\tnone\t-\nutf8only\toff\t-\n'
	g_destination_operating_system="FreeBSD"
	zxfer_property_test_run_transfer "tank/src"
	assertEquals "FreeBSD destinations treat aclmode as read-only." \
		"MUTATE set compression=lz4 backup/dst" "$(cat "$TRANSFER_MUTATE_LOG")"
	assertEquals "type,readonly,mountpoint,volsize" "$ZXFER_BASE_READONLY_PROPERTIES"

	g_destination_operating_system="SunOS"
	zxfer_property_test_run_transfer "tank/src"
	assertEquals "MUTATE set aclmode=passthrough compression=lz4 backup/dst" "$(cat "$TRANSFER_MUTATE_LOG")"
}

test_transfer_properties_does_not_capture_backup_metadata_before_success() {
	zxfer_property_test_default_rows
	TRANSFER_DST_ROWS='type\tfilesystem\t-\ncompression\tgzip\tlocal\natime\toff\tlocal\nreadonly\toff\tlocal\nmountpoint\t/mnt\tlocal\ncasesensitivity\tsensitive\t-\nnormalization\tnone\t-\nutf8only\toff\t-\n'
	TRANSFER_MUTATE_STATUS=1
	g_option_k_backup_property_mode=1
	output=$(
		zxfer_append_backup_metadata_record() { printf 'unexpected backup_append\n'; }
		zxfer_property_test_run_transfer "tank/src"
	)
	assertEquals 1 "$?"
	assertEquals "THROW Error when setting properties on destination filesystem.|1" "$output"
}

test_transfer_properties_skip_backup_capture_flag_suppresses_capture() {
	zxfer_property_test_default_rows
	g_option_k_backup_property_mode=1
	output=$(
		zxfer_append_backup_metadata_record() { printf 'backup_append %s\n' "$1"; }
		zxfer_property_test_run_transfer "tank/src" 1
		zxfer_property_test_run_transfer "tank/src" 0
	)
	assertEquals "backup_append tank/src" "$output"
}

test_transfer_properties_uses_restored_backup_properties_in_restore_mode() {
	zxfer_property_test_default_rows
	g_option_e_restore_property_mode=1
	ZXFER_TEST_BACKUP_SOURCE_ROOT="tank/src"
	ZXFER_TEST_BACKUP_DESTINATION_ROOT="backup/dst"
	g_restored_backup_file_contents=$(zxfer_test_render_current_backup_metadata_contents \
		"$(zxfer_test_backup_metadata_row "." "compression=gzip=local,casesensitivity=sensitive=-,normalization=none=-,utf8only=off=-")")
	zxfer_property_test_run_transfer "tank/src"
	assertEquals 0 "$?"
	assertEquals "The restored view replaces the live effective view; the live raw view still has the same creation-time properties." \
		"MUTATE set compression=gzip backup/dst" "$(cat "$TRANSFER_MUTATE_LOG")"
}

test_transfer_properties_resets_per_transfer_result_channels() {
	zxfer_property_test_default_rows
	(
		g_zxfer_plan_initial_set_result="stale"
		g_zxfer_source_pvs_raw="stale"
		zxfer_probe_destination_existence() {
			g_zxfer_destination_exists_result=${TRANSFER_DEST_EXISTS:-1}
		}
		zxfer_run_zfs_cmd_for_role() { zxfer_property_test_transfer_reads "$@"; }
		zxfer_run_destination_zfs_cmd() { :; }
		TRANSFER_READ_LOG=/dev/null
		zxfer_transfer_properties "tank/src"
		l_raw_has_type=no
		case "$g_zxfer_source_pvs_raw" in
		type=filesystem=-*) l_raw_has_type=yes ;;
		esac
		printf 'diff=<%s> raw_has_type=%s\n' "$g_zxfer_plan_initial_set_result" "$l_raw_has_type"
	) >"$TEST_TMPDIR/transfer_reset.out"
	assertEquals "diff=<> raw_has_type=yes" "$(cat "$TEST_TMPDIR/transfer_reset.out")"
}
