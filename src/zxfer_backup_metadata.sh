#!/bin/sh
# BSD HEADER START
# This file is part of zxfer project.

# Copyright (c) 2024-2026 Aldo Gonzalez
# Copyright (c) 2013-2019 Allan Jude <allanjude@freebsd.org>
# Copyright (c) 2010,2011 Ivan Nash Dreckman
# Copyright (c) 2007,2008 Constantin Gonzalez
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:

#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright notice,
#       this list of conditions and the following disclaimer in the documentation
#       and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# BSD HEADER END

# shellcheck shell=sh disable=SC2034,SC2154

################################################################################
# PROPERTY BACKUP METADATA: -k CAPTURE/WRITE AND -e RESTORE
################################################################################

# Module contract:
# owns globals: g_backup_storage_root (validated once per session), the
#   buffered -k rows in g_backup_file_contents ("<source-relative path>TAB
#   <properties>" lines), the -e roots and row index
#   (g_zxfer_backup_restore_*), the forwarded
#   provenance memo g_zxfer_backup_forwarded_*, reusable read staging
#   g_zxfer_backup_read_file, and the checked g_zxfer_backup_view_result.
#   Row lookups publish g_zxfer_property_row_payload_result;
#   dry-run previews publish quoting's g_zxfer_shell_command_result directly.
# reads globals: backup options, source and destination roots, -O/-T host
#   specs, g_zxfer_secure_path, path-security helpers, the ssh transport, and
#   the property state module's row writer and reader. Each metadata view
#   owns a private directory, independent of per-pass property tables.
# returns via stdout: metadata filenames, validated write-buffer rows, and
#   rendered remote sh programs. Read payloads stay in private row files.
#
# Layout: ZXFER_BACKUP_DIR/<source>/.zxfer_backup_info.v2/h/<chunks>/
# .zxfer_backup_info.v2, <chunks> being the hex of "<source>\n<destination>"
# split into 48-character components. A live -k run buffers one row per
# dataset and publishes two files once at the end of the run (and once after
# a post-seed property pass): the exact-pair file under the source root and
# a forwarded alias keyed by the destination root so a later -k hop from
# that destination forwards the original provenance. Each dataset takes its
# row from the nearest alias at or above it that has one. Both 0600 files
# are staged before either is renamed. If the second publish fails, restore
# the first file from its adjacent recovery copy (or remove a newly created
# file). Each rename is atomic; the pair is not a crash-atomic transaction.
#
# Protections, enforced locally and in the rendered remote programs: an
# absolute single-line ZXFER_BACKUP_DIR; no symlinked path component (a
# symlink directly under / such as macOS /var is trusted since only root can
# create entries there); directories 0700 and owned by root or the effective
# uid; files written 0600 and read only when they are regular 0600 files with
# that owner inside a directory other users cannot modify; a symlinked target
# is never followed; readers require the header and #format_version:2 and
# only ever look under ZXFER_BACKUP_DIR; remote status is captured through
# the probe files and fails closed; values travel as argv or file contents,
# never through eval.

ZXFER_BACKUP_METADATA_HEADER_LINE="#zxfer property backup file"
ZXFER_BACKUP_METADATA_FORMAT_VERSION="2"
# Lookup statuses, one vocabulary for the file check, the candidate lookup,
# the forwarded rows and the -e row lookup (the messages are in
# zxfer_throw_backup_candidate_failure and zxfer_transfer_properties):
#   0  one row for the dataset pair      6  header missing or not first
#   1  no metadata file                  7  unsupported #format_version
#   3  the pair does not resolve         8  valid file, no row for the pair
#   4  malformed row or root marker      9  more than one row for the pair
#   5  read or parse failure             11 no filename can be derived
# awk itself exits 2 on a fatal error, so no lookup outcome uses 2.

# Shared awk predicate: a row's property payload is "name=value=source" items
# joined by commas, none empty.
ZXFER_BACKUP_METADATA_PROPERTIES_AWK='
function validate_properties(properties, item_count, i, field_count) {
	if (properties == "")
		return 0
	item_count = split(properties, prop_items, ",")
	for (i = 1; i <= item_count; i++) {
		if (prop_items[i] == "")
			return 0
		field_count = split(prop_items[i], prop_fields, "=")
		if (field_count < 2 || prop_fields[1] == "" || prop_fields[field_count] == "")
			return 0
	}
	return 1
}'

# Purpose: Validate ZXFER_BACKUP_DIR (default /var/db/zxfer) once per session
# into g_backup_storage_root, ignoring any inherited internal value.
# Usage: zxfer_init_backup_storage_root, from session bootstrap; throws unless
# the root is a single-line absolute path. A dataset's storage directory is
# "$g_backup_storage_root/<dataset>".
zxfer_init_backup_storage_root() {
	l_init_backup_root=${ZXFER_BACKUP_DIR:-/var/db/zxfer}
	zxfer_value_is_single_line "$l_init_backup_root" ||
		zxfer_throw_error "Refusing to use ZXFER_BACKUP_DIR because the backup metadata root must be a single-line absolute path without control whitespace."
	case $l_init_backup_root in
	/*) g_backup_storage_root=$l_init_backup_root ;;
	*) zxfer_throw_error "Refusing to use backup metadata root \"$l_init_backup_root\" because ZXFER_BACKUP_DIR must be an absolute path." ;;
	esac
}

# Purpose: Reset the buffered rows, restore cache, forwarded memo, and result
# channels.
# Usage: zxfer_reset_backup_metadata_state, from session bootstrap.
zxfer_reset_backup_metadata_state() {
	g_backup_file_contents=""
	g_zxfer_backup_restore_index=""
	g_zxfer_backup_restore_source_root=""
	g_zxfer_backup_restore_destination_root=""
	g_zxfer_backup_restore_row_dir=""
	g_zxfer_backup_read_file=""
	g_zxfer_backup_restore_candidate_path_result=""
	g_zxfer_backup_view_result=""
	g_zxfer_backup_forwarded_roots=""
	g_zxfer_backup_forwarded_rows=""
	g_zxfer_backup_forwarded_properties=""
	g_zxfer_backup_forwarded_listed=0
	g_zxfer_backup_forwarded_listing=""
}

# Purpose: Print the metadata filename for a source/destination pair.
# Usage: zxfer_get_backup_metadata_filename SOURCE DESTINATION [legacy]. The
# current name is the chunked lossless identity path rendered by one awk
# pass; "legacy" prints the retired cksum-keyed name that lookups still read
# (never write). Fails (status 1) when the key cannot be derived.
zxfer_get_backup_metadata_filename() {
	l_filename_source=$1
	l_filename_destination=$2

	# shellcheck disable=SC2016  # awk programs should see literal field references.
	if [ "${3:-}" = legacy ]; then
		# Every retired writer hashed "SOURCE<LF>DESTINATION" with no final
		# newline (the command substitution that built it stripped one).
		l_filename_key=$(printf '%s\n%s' "$l_filename_source" "$l_filename_destination" |
			cksum | "${g_cmd_awk:-awk}" '$1 != "" && $2 != "" { print "k" $1 "." $2 }') || return 1
		[ -n "$l_filename_key" ] || return 1
		printf '%s.%s.%s\n' "$g_backup_file_extension" "${l_filename_source##*/}" "$l_filename_key"
		return 0
	fi

	# shellcheck disable=SC2016
	l_filename_key=$(printf '%s\n%s\n' "$l_filename_source" "$l_filename_destination" |
		LC_ALL=C "${g_cmd_awk:-awk}" '
BEGIN {
	for (i = 1; i < 256; i++)
		hex[sprintf("%c", i)] = sprintf("%02x", i)
}
{
	if (NR > 1)
		out = out "0a"
	n = length($0)
	for (i = 1; i <= n; i++)
		out = out hex[substr($0, i, 1)]
}
END {
	if (out == "")
		exit 1
	key_path = "h"
	for (i = 1; i <= length(out); i += 48)
		key_path = key_path "/" substr(out, i, 48)
	print key_path
}') || return 1
	[ -n "$l_filename_key" ] || return 1
	printf '%s.v2/%s/%s.v2\n' "$g_backup_file_extension" "$l_filename_key" "$g_backup_file_extension"
}

# Purpose: Validate, deduplicate, and print the buffered record list.
# Usage: Called once at the write boundary. Appends are plain O(1) string
# appends, so this is where every row is format-checked and where duplicate
# keys (repeated property passes, -Y iterations, post-seed reconciles)
# collapse newest-row-wins in first-appearance order. Returns 1 for a
# malformed row.
zxfer_validate_backup_metadata_record_list() {
	l_existing_records=$1

	# shellcheck disable=SC2016  # awk program should see literal field references.
	printf '%s\n' "$l_existing_records" |
		"${g_cmd_awk:-awk}" "$ZXFER_BACKUP_METADATA_PROPERTIES_AWK"'
{
	if ($0 == "")
		next
	tab = index($0, "\t")
	if (tab <= 0)
		exit 3
	current_key = substr($0, 1, tab - 1)
	current_properties = substr($0, tab + 1)
	if (current_key == "" || !validate_properties(current_properties))
		exit 3
	if (!(current_key in row_properties)) {
		row_count++
		row_keys[row_count] = current_key
	}
	row_properties[current_key] = current_properties
}
END {
	for (row_index = 1; row_index <= row_count; row_index++) {
		line = row_keys[row_index] "\t" row_properties[row_keys[row_index]]
		if (output == "")
			output = line
		else
			output = output "\n" line
	}
	printf "%s\n", output
}' || return 1
}

# Purpose: Append one source-root-relative row to the in-memory buffer.
# Usage: Called by the capture helper (and directly by tests). Duplicate keys
# are legitimate transient state; they collapse at the write boundary.
zxfer_append_backup_metadata_record() {
	l_append_metadata_source=$1
	l_append_metadata_properties=$2
	l_append_metadata_root=${g_initial_source:-$l_append_metadata_source}

	if [ "$l_append_metadata_source" = "$l_append_metadata_root" ]; then
		l_append_metadata_key=.
	else
		case "$l_append_metadata_source" in
		"$l_append_metadata_root"/*)
			l_append_metadata_key=${l_append_metadata_source#"$l_append_metadata_root"/}
			;;
		*)
			zxfer_throw_error "Backup metadata source dataset [$l_append_metadata_source] is outside source root [$l_append_metadata_root]."
			;;
		esac
	fi
	g_backup_file_contents="${g_backup_file_contents:+$g_backup_file_contents
}$l_append_metadata_key	$l_append_metadata_properties"
}

# Purpose: Buffer the backup row of a dataset whose property pass succeeded.
# Usage: zxfer_capture_backup_metadata_for_completed_transfer SOURCE
# LIVE_PROPERTIES; a forwarded provenance row from an earlier -k hop
# replaces the live properties. Nothing is written until
# zxfer_write_backup_properties runs.
zxfer_capture_backup_metadata_for_completed_transfer() {
	[ "${g_option_k_backup_property_mode:-0}" -eq 1 ] || return 0

	if zxfer_resolve_forwarded_backup_metadata "$1"; then
		zxfer_append_backup_metadata_record "$1" "$g_zxfer_backup_forwarded_properties"
	else
		zxfer_append_backup_metadata_record "$1" "$2"
	fi
}

# Purpose: Find the forwarded provenance row of one dataset: the nearest
# alias at or above it that has a row for it wins.
# Usage: zxfer_resolve_forwarded_backup_metadata DATASET; returns 0 with the
# row in g_zxfer_backup_forwarded_properties, 1 when no alias has one. A
# duplicate row fails closed.
zxfer_resolve_forwarded_backup_metadata() {
	g_zxfer_backup_forwarded_properties=""
	l_forwarded_root=$1
	while :; do
		l_forwarded_key=$l_forwarded_root$ZXFER_TAB$1$ZXFER_TAB
		if zxfer_load_forwarded_backup_alias "$l_forwarded_root"; then
			# An alias without a row for the dataset lets the walk go on.
			case $ZXFER_LF$g_zxfer_backup_forwarded_rows in
			*"$ZXFER_LF$l_forwarded_key"*) break ;;
			esac
		fi
		case $l_forwarded_root in
		*/*) l_forwarded_root=${l_forwarded_root%/*} ;;
		*) return 1 ;;
		esac
	done

	# Split each compact memo once, as the property index reader does.
	# Trimming a whole index at a key costs bash the square of its length.
	# Keep DATASET as the first argument for diagnostics after either split.
	zxfer_split_begin "$ZXFER_LF"
	# shellcheck disable=SC2086  # Intentional line splitting, globbing disabled.
	set -- "$1" $g_zxfer_backup_forwarded_rows
	zxfer_split_end
	for l_forwarded_row; do
		case $l_forwarded_row in
		"$l_forwarded_key"*)
			l_forwarded_row=${l_forwarded_row#"$l_forwarded_key"}
			break
			;;
		esac
	done
	zxfer_split_begin "$ZXFER_LF"
	# shellcheck disable=SC2086  # Intentional line splitting, globbing disabled.
	set -- "$1" $g_zxfer_backup_forwarded_roots
	zxfer_split_end
	for l_forwarded_path; do
		case $l_forwarded_path in
		"$l_forwarded_root$ZXFER_TAB"*)
			l_forwarded_path=${l_forwarded_path#"$l_forwarded_root$ZXFER_TAB"}
			break
			;;
		esac
	done
	l_forwarded_status=9
	if [ "$l_forwarded_row" != - ]; then
		if zxfer_read_property_row "$l_forwarded_row" "${l_forwarded_path#*"$ZXFER_TAB"}"; then
			g_zxfer_backup_forwarded_properties=$g_zxfer_property_row_payload_result
			return 0
		fi
		l_forwarded_status=5
	fi
	zxfer_throw_backup_candidate_failure "$l_forwarded_status" "${l_forwarded_path%%"$ZXFER_TAB"*}" \
		"$1" "Forwarded backup property file"
}

# Purpose: Load ROOT's forwarded alias (the file keyed by ROOT/ROOT on the
# source side, -O host or local) into the run's memo, once per run.
# Usage: zxfer_load_forwarded_backup_alias ROOT; returns 0 when ROOT has an
# alias, 1 when it has none. Every root looked up is remembered in
# g_zxfer_backup_forwarded_roots ("ROOT<TAB>ALIAS_PATH<TAB>ROW_DIRECTORY"
# lines, or "ROOT<TAB>" without an alias), and every alias's row references
# in g_zxfer_backup_forwarded_rows ("ROOT<TAB>DATASET<TAB>ROW" lines,
# ROW is "-" for a dataset with more than one row), so each root is
# read at most once. A root without a storage directory is ruled out without
# a read; an unreadable or invalid alias fails closed.
zxfer_load_forwarded_backup_alias() {
	case $ZXFER_LF$g_zxfer_backup_forwarded_roots$ZXFER_LF in
	*"$ZXFER_LF$1$ZXFER_TAB$ZXFER_LF"*) return 1 ;;
	*"$ZXFER_LF$1$ZXFER_TAB"*) return 0 ;;
	esac

	l_alias_dir=$g_backup_storage_root/$1
	l_alias_status=1
	# A symlinked directory counts as present: the lookup then refuses it.
	if [ -n "$g_option_O_origin_host" ]; then
		[ "${g_zxfer_backup_forwarded_listed:-0}" -eq 1 ] ||
			zxfer_list_remote_backup_storage_dirs
		case $ZXFER_LF$g_zxfer_backup_forwarded_listing$ZXFER_LF in
		*"$ZXFER_LF$1$ZXFER_LF"*) l_alias_status=0 ;;
		esac
	elif [ -d "$l_alias_dir" ] || [ -L "$l_alias_dir" ]; then
		l_alias_status=0
	fi
	if [ "$l_alias_status" -eq 0 ]; then
		zxfer_try_backup_restore_candidate "$l_alias_dir" "$1" "$1" "$1" "$1" \
			"$g_option_O_origin_host" source "$1"
		l_alias_status=$?
	fi
	l_alias_path=$g_zxfer_backup_restore_candidate_path_result
	# Status 8, an alias without a row for ROOT itself (-k writes one when -x
	# excludes the source root), still forwards its other rows.
	case $l_alias_status in
	0 | 8) ;;
	1)
		g_zxfer_backup_forwarded_roots=${g_zxfer_backup_forwarded_roots:+$g_zxfer_backup_forwarded_roots$ZXFER_LF}$1$ZXFER_TAB
		return 1
		;;
	*) zxfer_throw_backup_candidate_failure "$l_alias_status" "$l_alias_path" "$1" "Forwarded backup property file" ;;
	esac

	# The same validated view serves -e and forwarding. Its private directory
	# survives property-pass resets; the memo holds row names, not payloads.
	l_alias_dir=${g_zxfer_backup_view_result%%"$ZXFER_LF"*}
	l_alias_rows=${g_zxfer_backup_view_result#*"$ZXFER_LF"}
	l_alias_rows=${l_alias_rows#*"$ZXFER_LF"}
	l_alias_rows=${l_alias_rows#*"$ZXFER_LF"}
	g_zxfer_backup_forwarded_rows=${g_zxfer_backup_forwarded_rows:+$g_zxfer_backup_forwarded_rows$ZXFER_LF}$l_alias_rows
	g_zxfer_backup_forwarded_roots=${g_zxfer_backup_forwarded_roots:+$g_zxfer_backup_forwarded_roots$ZXFER_LF}$1$ZXFER_TAB$l_alias_path$ZXFER_TAB$l_alias_dir
	zxfer_echoV "Forwarding backup provenance from $l_alias_path"
	return 0
}

# Purpose: Learn in one -O round trip which datasets on the source root's
# path and below it have a storage directory on the origin host, so
# forwarded lookups skip the rest.
# Usage: zxfer_list_remote_backup_storage_dirs; sets
# g_zxfer_backup_forwarded_listing (dataset names, one per line) and
# g_zxfer_backup_forwarded_listed=1, or throws.
zxfer_list_remote_backup_storage_dirs() {
	l_listing_script=$(zxfer_build_remote_backup_storage_listing_cmd \
		"$g_initial_source" "$g_option_O_origin_host") ||
		zxfer_throw_error "Failed to render the backup metadata listing for $g_option_O_origin_host." "$?"
	zxfer_run_remote_backup_script "$g_option_O_origin_host" "$l_listing_script" source \
		"listing backup metadata under $g_backup_storage_root/$g_initial_source" \
		backup-metadata '9[27]'
	l_listing_status=$?
	if [ "$l_listing_status" -ne 0 ]; then
		zxfer_emit_remote_probe_failure_message >&2
		zxfer_throw_error "Failed to list backup metadata under $g_backup_storage_root/$g_initial_source on $g_option_O_origin_host."
	fi
	g_zxfer_backup_forwarded_listing=$g_zxfer_remote_probe_stdout
	g_zxfer_backup_forwarded_listed=1
}

# Purpose: Try the current and then the retired cksum-keyed filename for one
# source/destination pair and validate what is found.
# Usage: zxfer_try_backup_restore_candidate DIR FILENAME_SOURCE
# FILENAME_DESTINATION EXPECTED_SOURCE EXPECTED_DESTINATION [HOST]
# [PROFILE_SIDE] [ALIAS_ROOT]. Returns 0 with a checked view, or 8 when the
# pair has no row (aliases still publish a view on 8). The view is in
# g_zxfer_backup_view_result. Returns 1 when neither file exists; the file check's 3, 4, 6, 7 or 9; 5 for a read failure or any
# other check status; 11 when no filename can be derived. The path examined
# last is published in g_zxfer_backup_restore_candidate_path_result for error
# messages.
zxfer_try_backup_restore_candidate() {
	g_zxfer_backup_restore_candidate_path_result=""
	g_zxfer_backup_view_result=""

	for l_candidate_kind in current legacy; do
		if ! l_candidate_name=$(zxfer_get_backup_metadata_filename \
			"$2" "$3" "$l_candidate_kind"); then
			[ "$l_candidate_kind" = legacy ] || return 11
			break
		fi
		l_candidate_path=$1/$l_candidate_name
		if [ "$l_candidate_kind" = current ]; then
			g_zxfer_backup_restore_candidate_path_result=$l_candidate_path
			zxfer_ensure_runtime_artifact_file "$g_zxfer_backup_read_file" backup-read || return 5
			g_zxfer_backup_read_file=$g_zxfer_runtime_artifact_path_result
			l_candidate_file=$g_zxfer_backup_read_file
		fi
		if [ "${6:-}" = "" ]; then
			zxfer_read_local_backup_file "$l_candidate_path" "$l_candidate_file"
		else
			zxfer_read_remote_backup_file "${6:-}" "$l_candidate_path" \
				"${7:-}" "$l_candidate_file"
		fi
		l_candidate_read_status=$?
		if [ "$l_candidate_kind" = current ] || [ "$l_candidate_read_status" -ne 4 ]; then
			g_zxfer_backup_restore_candidate_path_result=$l_candidate_path
		fi
		case $l_candidate_read_status in
		0) ;;
		4) continue ;;
		*) return 5 ;;
		esac
		zxfer_load_backup_metadata_view "$l_candidate_file" \
			"$4" "$5" "${8:-}"
		l_candidate_match_status=$?
		case $l_candidate_match_status in
		0 | 3 | 4 | 6 | 7 | 8 | 9) return "$l_candidate_match_status" ;;
		*) return 5 ;;
		esac
	done
	return 1
}

# Purpose: Raise the structured error for a failed candidate lookup.
# Usage: zxfer_throw_backup_candidate_failure STATUS PATH DATASET LABEL
# [USAGE]; STATUS is a lookup status, LABEL names the file kind in messages
# and a non-empty USAGE routes the operator-facing lookup failures through
# the usage error.
zxfer_throw_backup_candidate_failure() {
	l_candidate_failure_status=$1
	l_candidate_failure_path=$2
	l_candidate_failure_dataset=$3
	l_candidate_failure_label=$4
	l_candidate_failure_usage=${5:-}

	case $l_candidate_failure_status in
	1) zxfer_throw_error_with_usage "Cannot find backup property file. Ensure that it
exists under the source-dataset-relative tree inside ZXFER_BACKUP_DIR." ;;
	9) l_candidate_failure_message="$l_candidate_failure_label $l_candidate_failure_path contains multiple relative rows for source dataset $l_candidate_failure_dataset. Remove the ambiguous rows or restore from a specific exact backup path." ;;
	3 | 8) l_candidate_failure_message="$l_candidate_failure_label $l_candidate_failure_path does not contain a current-format relative row for source dataset $l_candidate_failure_dataset." ;;
	4) l_candidate_failure_message="$l_candidate_failure_label $l_candidate_failure_path is malformed. Expected current-format relative-path and properties rows." ;;
	6) l_candidate_failure_message="$l_candidate_failure_label $l_candidate_failure_path does not start with the required zxfer backup metadata header." ;;
	7) l_candidate_failure_message="$l_candidate_failure_label $l_candidate_failure_path does not declare supported zxfer backup metadata format version #format_version:$ZXFER_BACKUP_METADATA_FORMAT_VERSION." ;;
	11) zxfer_throw_error "Failed to derive backup metadata filename for source dataset [$l_candidate_failure_dataset]." ;;
	*) zxfer_throw_error "Failed to read $(printf '%s' "$l_candidate_failure_label" | tr '[:upper:]' '[:lower:]') $l_candidate_failure_path." ;;
	esac
	if [ -n "$l_candidate_failure_usage" ]; then
		zxfer_throw_error_with_usage "$l_candidate_failure_message"
	fi
	zxfer_throw_error "$l_candidate_failure_message"
}

# Purpose: Load the exact-pair restore metadata for -e, failing closed before
# any dataset work: validate the whole file once, then store its rows so each
# dataset looks its row up (zxfer_find_restored_backup_properties).
# Usage: zxfer_get_backup_properties, from replication startup. The file is
# keyed by the source root and CLI destination under ZXFER_BACKUP_DIR on the
# source side; child datasets restore from relative rows of that one file.
zxfer_get_backup_properties() {
	zxfer_set_failure_stage "backup metadata read"

	zxfer_map_destination_dataset "$g_initial_source"
	l_restore_destination_root=$g_zxfer_destination_dataset_result
	l_restore_status=0
	zxfer_try_backup_restore_candidate "$g_backup_storage_root/$g_initial_source" \
		"$g_initial_source" "$g_destination" \
		"$g_initial_source" "$l_restore_destination_root" "$g_option_O_origin_host" source ||
		l_restore_status=$?
	if [ "$l_restore_status" -eq 0 ]; then
		# Retire only the previous restore view; forwarded aliases have session
		# lifetimes and use separate directories. Keep repeated -Y passes bounded.
		zxfer_cleanup_runtime_artifact_path "$g_zxfer_backup_restore_row_dir" ||
			zxfer_throw_backup_candidate_failure 5 "$g_zxfer_backup_restore_candidate_path_result" \
				"$g_initial_source" "Backup property file" usage
		g_zxfer_backup_restore_row_dir=${g_zxfer_backup_view_result%%"$ZXFER_LF"*}
		l_restore_view=${g_zxfer_backup_view_result#*"$ZXFER_LF"}
		g_zxfer_backup_restore_source_root=${l_restore_view%%"$ZXFER_LF"*}
		l_restore_view=${l_restore_view#*"$ZXFER_LF"}
		g_zxfer_backup_restore_destination_root=${l_restore_view%%"$ZXFER_LF"*}
		g_zxfer_backup_restore_index=$ZXFER_LF${l_restore_view#*"$ZXFER_LF"}$ZXFER_LF
	fi
	[ "$l_restore_status" -eq 0 ] ||
		zxfer_throw_backup_candidate_failure "$l_restore_status" \
			"$g_zxfer_backup_restore_candidate_path_result" "$g_initial_source" \
			"Backup property file" usage
}

# Purpose: Find the -e row of one source/destination pair; both datasets
# must map to the same path relative to the file's #source_root and
# #destination_root.
# Usage: zxfer_find_restored_backup_properties SOURCE DESTINATION; publishes
# the properties in g_zxfer_property_row_payload_result and returns 0,
# or the lookup status 3 (the pair does not resolve, or no file was loaded),
# 8 (no row), 9 (more than one row) or 5 (the row cannot be read).
zxfer_find_restored_backup_properties() {
	g_zxfer_property_row_payload_result=""
	[ -n "$g_zxfer_backup_restore_source_root" ] &&
		[ -n "$g_zxfer_backup_restore_destination_root" ] || return 3
	case $1 in
	"$g_zxfer_backup_restore_source_root") l_restore_key=. ;;
	"$g_zxfer_backup_restore_source_root"/?*)
		l_restore_key=${1#"$g_zxfer_backup_restore_source_root"/}
		;;
	*) return 3 ;;
	esac
	case $2 in
	"$g_zxfer_backup_restore_destination_root") l_restore_destination_key=. ;;
	"$g_zxfer_backup_restore_destination_root"/?*)
		l_restore_destination_key=${2#"$g_zxfer_backup_restore_destination_root"/}
		;;
	*) return 3 ;;
	esac
	[ "$l_restore_key" = "$l_restore_destination_key" ] || return 3

	zxfer_find_property_row "$g_zxfer_backup_restore_index" "$l_restore_key" || return 8
	[ "$g_zxfer_property_row_result" != - ] || return 9
	zxfer_read_property_row "$g_zxfer_property_row_result" "$g_zxfer_backup_restore_row_dir" || return 5
}

# Purpose: Validate a staged metadata file, then materialize a private row
# view shared by -e and forwarded aliases. The first pass retains only keys
# and counts; the second writes one payload at a time after validation.
# Usage: zxfer_load_backup_metadata_view FILE SOURCE DESTINATION [ALIAS_ROOT].
# Publishes g_zxfer_backup_view_result: directory, source root, destination
# root, then index lines, terminated by LF. Restore lines are ROW<TAB>KEY;
# alias lines are ALIAS_ROOT<TAB>DATASET<TAB>ROW. Duplicates use "-".
# Returns the lookup
# status; publishes on 0, or on 8 for an alias without its requested row.
# A read/write/allocation failure is 5.
zxfer_load_backup_metadata_view() {
	g_zxfer_backup_view_result=""
	[ -r "$1" ] || return 5
	# shellcheck disable=SC2016
	l_view_index=$(ZXFER_AWK_ALIAS_ROOT=${4:-} "${g_cmd_awk:-awk}" \
		-v expected_header="$ZXFER_BACKUP_METADATA_HEADER_LINE" \
		-v expected_format_version="$ZXFER_BACKUP_METADATA_FORMAT_VERSION" \
		-v expected_source="$2" \
		-v expected_destination="$3" \
		"$ZXFER_BACKUP_METADATA_PROPERTIES_AWK"'
BEGIN { alias_root = ENVIRON["ZXFER_AWK_ALIAS_ROOT"] }
function relative_path(root, dataset, prefix) {
	if (root == "" || dataset == "")
		return ""
	if (dataset == root)
		return "."
	prefix = root "/"
	if (substr(dataset, 1, length(prefix)) == prefix)
		return substr(dataset, length(prefix) + 1)
	return "__ZXFER_NO_MATCH__"
}
# An "exit" in a rule still runs END, which reports format_status first.
NR == 1 {
	if ($0 != expected_header) {
		format_status = 6
		exit
	}
	next
}
$0 == expected_header {
	format_status = 6
	exit
}
index($0, "#format_version:") == 1 {
	if (format_seen || substr($0, length("#format_version:") + 1) != expected_format_version) {
		format_status = 7
		exit
	}
	format_seen = 1
	next
}
{
	if (index($0, "#source_root:") == 1) {
		source_root_count++
		source_root = substr($0, length("#source_root:") + 1)
		next
	}
	if (index($0, "#destination_root:") == 1) {
		destination_root_count++
		destination_root = substr($0, length("#destination_root:") + 1)
		next
	}
	if ($0 == "" || substr($0, 1, 1) == "#")
		next
	# A row before #format_version reads as a file without the header.
	if (!format_seen) {
		format_status = 6
		exit
	}

	tab = index($0, "\t")
	if (tab <= 0) {
		malformed_count++
		next
	}
	row_key = substr($0, 1, tab - 1)
	props = substr($0, tab + 1)
	if (row_key == "" || row_key ~ /^\// || row_key ~ /\/$/ || !validate_properties(props)) {
		malformed_count++
		next
	}
	if (!(row_key in row_count))
		keys[++key_count] = row_key
	row_count[row_key]++
}
END {
	if (NR == 0)
		exit 6
	if (format_status)
		exit format_status
	if (!format_seen)
		exit 7
	if (source_root_count != 1 || destination_root_count != 1 || source_root == "" || destination_root == "")
		exit 4
	expected_source_key = relative_path(source_root, expected_source)
	expected_destination_key = relative_path(destination_root, expected_destination)
	if (expected_source_key == "" || expected_destination_key == "" ||
		expected_source_key == "__ZXFER_NO_MATCH__" ||
		expected_destination_key == "__ZXFER_NO_MATCH__" ||
		expected_source_key != expected_destination_key)
		exit 3
	if (malformed_count > 0)
		exit 4
	if (row_count[expected_source_key] > 1)
		exit 9
	print source_root
	print destination_root
	for (i = 1; i <= key_count; i++) {
		row = (row_count[keys[i]] > 1) ? "-" : ("r" i)
		if (alias_root == "")
			print row "\t" keys[i]
		else {
			dataset = (keys[i] == ".") ? source_root : (source_root "/" keys[i])
			print alias_root "\t" dataset "\t" row
		}
	}
	exit (row_count[expected_source_key] == 0) ? 8 : 0
}' "$1")
	l_view_status=$?
	case $l_view_status in
	0 | 8) ;;
	3 | 4 | 6 | 7 | 9) return "$l_view_status" ;;
	*) return 5 ;;
	esac
	# -e rejects a missing requested row before any row-store work. Aliases
	# may omit their root row and still forward children, so materialize those.
	[ "$l_view_status" -ne 8 ] || [ -n "${4:-}" ] || return 8
	# Restore used to prepare property scratch here, before discovery and -d.
	# Keep that allocation failure boundary even though its rows are separate.
	if [ -z "${4:-}" ]; then
		zxfer_prepare_property_read_files || return 5
	fi
	zxfer_create_private_temp_dir backup-rows || return 5
	l_view_dir=$g_zxfer_runtime_artifact_path_result
	# Files stay private even when the caller's run umask is permissive. The
	# shared row writer checks close(), including delayed disk-full failures.
	# shellcheck disable=SC2016
	(
		umask 077
		ZXFER_AWK_ROW_DIR=$l_view_dir ZXFER_AWK_ROW_PREFIX=r \
			ZXFER_AWK_ROW_BASE=0 "${g_cmd_awk:-awk}" "$ZXFER_PROPERTY_AWK_LIB"'
$0 == "" || substr($0, 1, 1) == "#" { next }
{
	tab = index($0, "\t")
	key = substr($0, 1, tab - 1)
	if (!(key in seen)) {
		seen[key] = 1
		store_row(substr($0, tab + 1))
	}
}' "$1"
	) || return 5
	g_zxfer_backup_view_result=$l_view_dir$ZXFER_LF$l_view_index$ZXFER_LF
	return "$l_view_status"
}

# Purpose: Validate or create the -k backup root before any replication work,
# so an unsafe path fails before ZFS operations.
# Usage: zxfer_check_backup_storage_dir_if_needed, at the start of each pass;
# under -n it only prints the commands. Returns a renderer's failure status.
zxfer_check_backup_storage_dir_if_needed() {
	[ "${g_option_k_backup_property_mode:-0}" -eq 1 ] || return 0

	if [ "$g_option_T_target_host" = "" ]; then
		if [ "$g_option_n_dryrun" -eq 1 ]; then
			zxfer_render_shell_command_from_argv mkdir -p "$g_backup_storage_root"
			l_check_mkdir=$g_zxfer_shell_command_result
			zxfer_render_shell_command_from_argv chmod 700 "$g_backup_storage_root"
			zxfer_echov "Dry run: umask 077; $l_check_mkdir; $g_zxfer_shell_command_result"
			return 0
		fi
		zxfer_ensure_local_backup_dir "$g_backup_storage_root"
		return 0
	fi

	l_check_script=$(zxfer_build_remote_backup_dir_prepare_cmd \
		"$g_backup_storage_root" "$g_option_T_target_host") || return "$?"
	if [ "$g_option_n_dryrun" -eq 1 ]; then
		zxfer_render_remote_backup_dry_run_shell_command "$g_option_T_target_host" \
			"$l_check_script" || return "$?"
		zxfer_echov "Dry run: $g_zxfer_shell_command_result"
		return 0
	fi
	if ! zxfer_run_remote_backup_script "$g_option_T_target_host" "$l_check_script" \
		destination "preparing backup directory $g_backup_storage_root" \
		backup-directory 92; then
		zxfer_emit_remote_probe_failure_message >&2
		zxfer_throw_error "Error preparing backup directory on $g_option_T_target_host."
	fi
}

# Purpose: Publish the buffered -k rows as the exact-pair metadata file plus
# the forwarded provenance alias, locally or on the -T host.
# Usage: zxfer_write_backup_properties, once after a post-seed property pass
# and once at run end. A dry run buffers no rows, so it only says so.
zxfer_write_backup_properties() {
	zxfer_set_failure_stage "backup metadata write"

	if [ -z "${g_backup_file_contents:-}" ]; then
		zxfer_echov "No property data collected; skipping backup write."
		return 0
	fi
	# Validate-once boundary: duplicate keys collapse newest-row-wins and any
	# malformed row fails before either file is touched.
	if ! l_write_records=$(zxfer_validate_backup_metadata_record_list "$g_backup_file_contents"); then
		zxfer_throw_error "Failed to validate buffered backup metadata records for chained backup provenance."
	fi
	g_backup_file_contents=$l_write_records

	zxfer_map_destination_dataset "$g_initial_source"
	l_write_destination_root=$g_zxfer_destination_dataset_result
	l_write_primary_name=$(zxfer_get_backup_metadata_filename "$g_initial_source" "$g_destination") ||
		zxfer_throw_error "Failed to derive backup metadata filename for source dataset [$g_initial_source]."
	l_write_primary_path=$g_backup_storage_root/$g_initial_source/$l_write_primary_name
	l_write_forwarded_name=$(zxfer_get_backup_metadata_filename "$l_write_destination_root" "$l_write_destination_root") ||
		zxfer_throw_error "Failed to derive forwarded backup metadata filename for destination dataset [$l_write_destination_root]."
	l_write_forwarded_path=$g_backup_storage_root/$l_write_destination_root/$l_write_forwarded_name
	zxfer_echov "Writing backup info to secure path $l_write_primary_path (dataset $g_initial_source)"
	l_write_date=$(date)
	l_write_contents=$(printf '%s\n' "$ZXFER_BACKUP_METADATA_HEADER_LINE" \
		"#format_version:$ZXFER_BACKUP_METADATA_FORMAT_VERSION" \
		"#version:$g_zxfer_version" \
		"#R options:$g_option_R_recursive" \
		"#N options:$g_option_N_nonrecursive" \
		"#source_root:$g_initial_source" \
		"#destination_root:$l_write_destination_root" \
		"#backup_date:$l_write_date" \
		"$g_backup_file_contents")

	l_write_status=0
	if [ "$g_option_T_target_host" = "" ]; then
		zxfer_ensure_local_backup_dir "${l_write_primary_path%/*}"
		zxfer_ensure_local_backup_dir "${l_write_forwarded_path%/*}"
		l_write_script=$(zxfer_build_backup_pair_write_cmd \
			"$l_write_primary_path" "$l_write_forwarded_path" \
			"$l_write_destination_root" cat) || return "$?"
		# /bin/sh, as for job shells, so the secure PATH need not list sh.
		/bin/sh -c "$l_write_script" <<EOF || l_write_status=$?
$l_write_contents
EOF
	else
		zxfer_resolve_cli_command_safe "$g_option_T_target_host" cat cat destination ||
			zxfer_throw_dependency_error "$g_zxfer_resolved_cli_command_result"
		l_write_remote_cat=$g_zxfer_resolved_cli_command_result
		# One program prepares both directories and then publishes the pair;
		# the file contents travel on stdin.
		l_write_script=$(
			for l_write_dir in "${l_write_primary_path%/*}" "${l_write_forwarded_path%/*}"; do
				zxfer_build_remote_backup_dir_prepare_cmd "$l_write_dir" \
					"$g_option_T_target_host" mktemp mv rm || exit
			done
			zxfer_build_backup_pair_write_cmd "$l_write_primary_path" \
				"$l_write_forwarded_path" "$l_write_destination_root" "$l_write_remote_cat"
		) || return "$?"
		zxfer_run_remote_backup_script "$g_option_T_target_host" "$l_write_script" destination \
			"writing backup metadata $l_write_primary_path" backup-write '9[28]' <<EOF || l_write_status=$?
$l_write_contents
EOF
		[ "$l_write_status" -eq 0 ] || zxfer_emit_remote_probe_failure_message >&2
	fi
	if [ "$l_write_status" -eq 98 ]; then
		zxfer_throw_error "Error writing backup file and restoring backup metadata rollback state. Inspect the reported paths under ZXFER_BACKUP_DIR for manual recovery."
	fi
	[ "$l_write_status" -eq 0 ] ||
		zxfer_throw_error "Error writing backup file. Is filesystem mounted?"
}

# Purpose: Create or validate one local backup directory.
# Usage: Refuses symlinked components, non-directories, and owners other
# than root or the effective uid; creates missing directories 0700 and
# re-applies 0700 so an operator-created directory is private too.
zxfer_ensure_local_backup_dir() {
	l_ensure_local_backup_dir=$1
	if l_ensure_local_backup_symlink=$(zxfer_find_symlink_path_component \
		"$l_ensure_local_backup_dir"); then
		if [ "$l_ensure_local_backup_symlink" = "$l_ensure_local_backup_dir" ]; then
			zxfer_throw_error "Refusing to use backup directory $l_ensure_local_backup_dir because it is a symlink."
		fi
		zxfer_throw_error "Refusing to use backup directory $l_ensure_local_backup_dir because path component $l_ensure_local_backup_symlink is a symlink."
	fi
	if [ -L "$l_ensure_local_backup_dir" ]; then
		zxfer_throw_error "Refusing to use backup directory $l_ensure_local_backup_dir because it is a symlink."
	fi
	if [ -e "$l_ensure_local_backup_dir" ] && [ ! -d "$l_ensure_local_backup_dir" ]; then
		zxfer_throw_error "Refusing to use backup directory $l_ensure_local_backup_dir because it is not a directory."
	fi
	if [ ! -d "$l_ensure_local_backup_dir" ]; then
		(umask 077 && mkdir -p "$l_ensure_local_backup_dir") ||
			zxfer_throw_error "Error creating secure backup directory $l_ensure_local_backup_dir."
	fi
	if ! l_ensure_local_backup_owner_uid=$(zxfer_get_path_owner_uid \
		"$l_ensure_local_backup_dir"); then
		zxfer_throw_error "Cannot determine the owner of backup directory $l_ensure_local_backup_dir."
	fi
	if ! zxfer_backup_owner_uid_is_allowed "$l_ensure_local_backup_owner_uid"; then
		l_ensure_local_backup_expected_owner=$(zxfer_describe_expected_backup_owner)
		zxfer_throw_error "Refusing to use backup directory $l_ensure_local_backup_dir because it is owned by UID $l_ensure_local_backup_owner_uid instead of $l_ensure_local_backup_expected_owner."
	fi
	if ! chmod 700 "$l_ensure_local_backup_dir"; then
		zxfer_throw_error "Error securing backup directory $l_ensure_local_backup_dir."
	fi
}

# Purpose: Read one local metadata file after the security checks.
# Usage: zxfer_read_local_backup_file PATH STAGING_FILE; copies to the
# already allocated private file, publishing no result on stdout.
# Returns 1 (symlink component, message on stderr), 4 (missing), the cat
# status on read failure, and throws when the file is not a 0600 regular
# file owned by root or the effective uid, or when its directory could be
# modified by other users (which would let them swap the file between the
# check and the read).
zxfer_read_local_backup_file() {
	l_read_local_path=$1

	zxfer_require_backup_metadata_path_without_symlinks "$l_read_local_path" || return 1
	if [ ! -f "$l_read_local_path" ]; then
		return 4
	fi
	if ! l_read_local_error=$(zxfer_check_secure_backup_file "$l_read_local_path"); then
		zxfer_throw_error "$l_read_local_error"
	fi
	case $l_read_local_path in
	?*/*) l_read_local_parent=${l_read_local_path%/*} ;;
	*) l_read_local_parent=/ ;;
	esac
	zxfer_validate_temp_root_candidate "$l_read_local_parent" >/dev/null ||
		zxfer_throw_error "Refusing to use backup metadata $l_read_local_path because its directory $l_read_local_parent is not a private directory owned by root or the current user."
	cat "$l_read_local_path" >|"$2"
}

# Purpose: Read one metadata file on a remote host through the rendered read
# program.
# Usage: zxfer_read_remote_backup_file HOST PATH PROFILE_SIDE STAGING_FILE.
# Same contract as the local reader: 0 after capture, 1 after a symlink
# refusal (remote stderr forwarded), 4 when missing, 5 on other failures;
# insecure ownership, mode, or directory throws with the documented text.
zxfer_read_remote_backup_file() {
	l_read_remote_host=$1
	l_read_remote_path=$2
	l_read_remote_profile_side=${3:-}

	l_read_remote_script=$(zxfer_build_remote_backup_read_cmd "$l_read_remote_path" \
		"$l_read_remote_host") || return "$?"
	zxfer_run_remote_backup_script "$l_read_remote_host" "$l_read_remote_script" \
		"$l_read_remote_profile_side" "reading backup metadata $l_read_remote_path" \
		backup-metadata '9[1-8]' "$4"
	l_read_remote_status=$?
	case $l_read_remote_status in
	0)
		return 0
		;;
	91) zxfer_throw_error "Refusing to use backup metadata $l_read_remote_path on $l_read_remote_host because its directory is not a private directory owned by root or the ssh user." ;;
	94) return 4 ;;
	95) zxfer_throw_error "Refusing to use backup metadata $l_read_remote_path on $l_read_remote_host because it is not owned by root or the ssh user." ;;
	96) zxfer_throw_error "Refusing to use backup metadata $l_read_remote_path on $l_read_remote_host because its permissions are not 0600." ;;
	97) zxfer_throw_error "Cannot determine ownership or permissions for backup metadata $l_read_remote_path on $l_read_remote_host." ;;
	98)
		zxfer_emit_remote_probe_failure_message >&2
		return 1
		;;
	esac
	zxfer_emit_remote_probe_failure_message >&2
	return 5
}

# Purpose: Run one rendered backup program on a host and translate the
# transport-level outcomes.
# Usage: zxfer_run_remote_backup_script HOST SCRIPT PROFILE_SIDE ACTION
# DEPENDENCY_LABEL OWN_STATUS_GLOB [STDOUT_FILE]. A metadata reader supplies
# its allocated capture file; other callers retain the string probe result.
# Standard input is inherited (writers feed
# the payload through a here-document). Returns 0 or a status matching
# OWN_STATUS_GLOB for the caller to interpret; a capture failure, a missing
# remote helper (99), or any other failure with stderr output throws here.
zxfer_run_remote_backup_script() {
	l_run_remote_host=$1
	l_run_remote_script=$2
	l_run_remote_profile_side=$3
	l_run_remote_action=$4
	l_run_remote_label=$5
	l_run_remote_own_statuses=$6

	zxfer_build_remote_sh_c_command "$l_run_remote_script" >/dev/null
	zxfer_capture_remote_probe_output "$l_run_remote_host" \
		"$g_zxfer_shell_command_result" "$l_run_remote_profile_side" "${7:-}"
	l_run_remote_status=$?
	if [ "${g_zxfer_remote_probe_capture_failed:-0}" -eq 1 ]; then
		zxfer_emit_remote_probe_failure_message >&2
		zxfer_throw_error "Failed to reload local remote helper capture while $l_run_remote_action on host $l_run_remote_host."
	fi
	# shellcheck disable=SC2254  # the caller's status set is a glob on purpose.
	case $l_run_remote_status in
	0 | $l_run_remote_own_statuses)
		return "$l_run_remote_status"
		;;
	99)
		zxfer_emit_remote_probe_failure_message >&2
		zxfer_throw_dependency_error "Required remote $l_run_remote_label helper dependency not found on host $l_run_remote_host in secure PATH ($g_zxfer_secure_path). Review prior stderr for the missing tool name."
		;;
	esac
	if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
		zxfer_emit_remote_probe_failure_message >&2
		zxfer_throw_error "Failed to contact host $l_run_remote_host while $l_run_remote_action. Review prior stderr for the transport or authentication error."
	fi
	return "$l_run_remote_status"
}

# Purpose: Emit a literal backup program body without expanding its shell text.
# Usage: zxfer_print_backup_script_body, with a quoted here-document on stdin.
# Caller values belong in the argv-quoted initialization before this body.
zxfer_print_backup_script_body() {
	while IFS= read -r l_backup_script_line || [ -n "$l_backup_script_line" ]; do
		printf '%s\n' "$l_backup_script_line"
	done
}

# Purpose: Render the shared prelude of every remote backup program: the
# secure PATH, the helper check zxfer_require_remote_backup_tool (exit 99)
# run on each TOOL, and the symlink walk over GUARD_PATH (exit 92 for a
# directory, 98 for a metadata file).
# Usage: zxfer_build_remote_backup_script_prelude HOST GUARD_PATH
# directory|metadata [TOOL...]; every rendered line ends a command so the
# program stays valid after newline collapsing.
zxfer_build_remote_backup_script_prelude() {
	l_prelude_host=$1
	l_prelude_guard_path=$2
	l_prelude_guard_kind=$3
	shift 3

	case $l_prelude_guard_kind in
	directory) l_prelude_guard_status=92 ;;
	metadata) l_prelude_guard_status=98 ;;
	*) return 1 ;;
	esac
	zxfer_render_shell_command_from_argv set -- "$g_zxfer_secure_path" \
		"$l_prelude_host" "$l_prelude_guard_path" "$l_prelude_guard_kind" \
		"$l_prelude_guard_status" "$@"
	printf '%s;\n' "$g_zxfer_shell_command_result"
	# The quoted body runs on the backup host. Only the argv prefix above
	# contains caller values; read/printf emits it without spawning cat.
	# The walk trusts symlinks directly under /: only root can create them,
	# and macOS keeps /var and /tmp there.
	zxfer_print_backup_script_body <<-'EOF'
		PATH=$1;
		export PATH;
		l_required_host=$2;
		l_scan_path=$3;
		l_scan_kind=$4;
		l_scan_status=$5;
		shift 5;
		zxfer_require_remote_backup_tool() {
		  l_required_tool=$1;
		  if command -v "$l_required_tool" >/dev/null 2>&1; then
		    return 0;
		  fi;
		  printf 'Required dependency "%s" not found on host %s in secure PATH (%s). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary.\n' "$l_required_tool" "$l_required_host" "$PATH" >&2;
		  exit 99;
		};
		for l_required_tool in "$@"; do
		  zxfer_require_remote_backup_tool "$l_required_tool";
		done;

		l_scan_rest=$l_scan_path;
		l_scan_candidate='';
		case "$l_scan_rest" in
		/*) l_scan_candidate=/; l_scan_rest=${l_scan_rest#/} ;;
		esac;
		while [ -n "$l_scan_rest" ]; do
		  l_scan_component=${l_scan_rest%%/*};
		  case "$l_scan_rest" in
		  */*) l_scan_rest=${l_scan_rest#*/} ;;
		  *) l_scan_rest='' ;;
		  esac;
		  [ -n "$l_scan_component" ] || continue;
		  case "$l_scan_candidate" in
		  '') l_scan_candidate=$l_scan_component ;;
		  /) l_scan_candidate=/$l_scan_component ;;
		  *) l_scan_candidate=$l_scan_candidate/$l_scan_component ;;
		  esac;
		  [ -L "$l_scan_candidate" ] || continue;
		  case "$l_scan_candidate" in
		  /*/*) ;;
		  /*) continue ;;
		  esac;
		  case "$l_scan_kind:$l_scan_candidate" in
		  directory:"$l_scan_path")
		    echo 'Refusing to use symlinked zxfer backup directory.' >&2;
		    ;;
		  metadata:"$l_scan_path")
		    echo "Refusing to use backup metadata $l_scan_path because it is a symlink." >&2;
		    ;;
		  *)
		    if [ "$l_scan_kind" = directory ]; then
		      echo "Refusing to use backup directory $l_scan_path because path component $l_scan_candidate is a symlink." >&2;
		    else
		      echo "Refusing to use backup metadata $l_scan_path because path component $l_scan_candidate is a symlink." >&2;
		    fi;
		    ;;
		  esac;
		  exit "$l_scan_status";
		done;
	EOF
}

# Purpose: Render the remote program that creates or validates one backup
# directory: symlink walk, not-a-directory check, mkdir -p under umask 077,
# chmod 700, and the root-or-ssh-user owner check.
# Usage: zxfer_build_remote_backup_dir_prepare_cmd DIR HOST [EXTRA_TOOL...];
# the program exits 99 for a missing helper and 92 for any other refusal.
zxfer_build_remote_backup_dir_prepare_cmd() {
	l_prepare_dir=$1
	l_prepare_host=$2
	shift 2

	# ls must not read a leading dash as an option.
	l_prepare_ls_path=$l_prepare_dir
	case $l_prepare_dir in
	-*) l_prepare_ls_path=./$l_prepare_dir ;;
	esac
	zxfer_build_remote_backup_script_prelude "$l_prepare_host" "$l_prepare_dir" \
		directory mkdir chmod id ls awk "$@" || return "$?"

	zxfer_render_shell_command_from_argv set -- "$l_prepare_dir" "$l_prepare_ls_path"
	printf '%s;\n' "$g_zxfer_shell_command_result"
	zxfer_print_backup_script_body <<-'EOF'
		l_prepare_dir=$1;
		l_prepare_ls_path=$2;
		if [ -e "$l_prepare_dir" ] && [ ! -d "$l_prepare_dir" ]; then
		  echo 'Backup path exists but is not a directory.' >&2;
		  exit 92;
		fi;
		umask 077;
		if ! mkdir -p "$l_prepare_dir"; then
		  echo 'Error creating secure backup directory.' >&2;
		  exit 92;
		fi;
		if ! chmod 700 "$l_prepare_dir"; then
		  echo 'Error securing backup directory.' >&2;
		  exit 92;
		fi;
		l_dir_uid=$(ls -ldn "$l_prepare_ls_path" 2>/dev/null | awk '$3 ~ /^[0-9]+$/ { print $3 }');
		if [ "$l_dir_uid" = '' ]; then
		  echo 'Unable to determine backup directory owner.' >&2;
		  exit 92;
		fi;
		if [ "$l_dir_uid" != 0 ] && [ "$l_dir_uid" != "$(id -u)" ]; then
		  echo 'Backup directory must be owned by root or the ssh user.' >&2;
		  exit 92;
		fi;
	EOF
}

# Purpose: Render the local or remote program that publishes the metadata
# pair from stdin (the complete primary file; the forwarded copy differs only
# in its #source_root line) into two directories already trusted and private.
# Usage: zxfer_build_backup_pair_write_cmd PRIMARY FORWARDED ROOT CAT_COMMAND,
# CAT_COMMAND is one executable, already shell-quoted. The program exits 92
# when publication fails and 98 when rollback fails too. Each rename is atomic;
# this is detected-failure rollback, not crash atomicity. Signals defer until
# each rename and its marker agree: a signal after the first publish rolls
# it back; after the second, both files stay live.
zxfer_build_backup_pair_write_cmd() {
	zxfer_render_shell_command_from_argv set -- "$1" "$2" "$3"
	printf '%s;\nl_pair_cat=%s;\n' "$g_zxfer_shell_command_result" "$4"
	zxfer_print_backup_script_body <<-'EOF'
		l_pair_primary=$1;
		l_pair_forwarded=$2;
		l_pair_root=$3;
		l_pair_stage='';
		l_pair_forwarded_stage='';
		l_pair_recovery='';
		l_pair_published=0;
		zxfer_finish_backup_pair() {
		  l_pair_exit=$?;
		  trap - 0;
		  trap '' HUP INT TERM;
		  if [ "$l_pair_published" -eq 1 ]; then
		    if [ -n "$l_pair_recovery" ]; then
		      if ! mv -f "$l_pair_recovery" "$l_pair_primary"; then
		        rm -f "$l_pair_primary" || :;
		        printf 'Backup metadata rollback failed; recover %s from %s.\n' "$l_pair_primary" "$l_pair_recovery" >&2;
		        l_pair_recovery='';
		        l_pair_exit=98;
		      fi;
		    elif ! rm -f "$l_pair_primary"; then
		      printf 'Backup metadata rollback failed; remove the newly published %s before retrying.\n' "$l_pair_primary" >&2;
		      l_pair_exit=98;
		    fi;
		  fi;
		  for l_pair_cleanup in "$l_pair_stage" "$l_pair_forwarded_stage" "$l_pair_recovery"; do
		    [ -z "$l_pair_cleanup" ] || rm -f "$l_pair_cleanup" || { [ "$l_pair_exit" -ne 0 ] || l_pair_exit=92; };
		  done;
		  exit "$l_pair_exit";
		};
		trap zxfer_finish_backup_pair 0;
		trap 'exit 129' HUP;
		trap 'exit 130' INT;
		trap 'exit 143' TERM;
		umask 077;
		for l_pair_target in "$l_pair_primary" "$l_pair_forwarded"; do
		  if [ -L "$l_pair_target" ]; then
		    printf 'Refusing to write backup metadata %s because it is a symlink.\n' "$l_pair_target" >&2;
		    exit 92;
		  fi;
		  if [ -e "$l_pair_target" ] && [ ! -f "$l_pair_target" ]; then
		    printf 'Refusing to write backup metadata %s because it is not a regular file.\n' "$l_pair_target" >&2;
		    exit 92;
		  fi;
		done;
		l_pair_stage=$(mktemp "${l_pair_primary%/*}/.zxfer-backup-write.XXXXXX") || exit 92;
		"$l_pair_cat" >"$l_pair_stage" && chmod 600 "$l_pair_stage" || exit 92;
		if [ "$l_pair_primary" = "$l_pair_forwarded" ]; then
		  [ ! -L "$l_pair_primary" ] && mv -f "$l_pair_stage" "$l_pair_primary" || exit 92;
		  exit 0;
		fi;
		l_pair_forwarded_stage=$(mktemp "${l_pair_forwarded%/*}/.zxfer-backup-write.XXXXXX") || exit 92;
		ZXFER_BACKUP_FORWARD_ROOT=$l_pair_root awk '
		  index($0, "#source_root:") == 1 { $0 = "#source_root:" ENVIRON["ZXFER_BACKUP_FORWARD_ROOT"] }
		  { print }
		' "$l_pair_stage" >"$l_pair_forwarded_stage" && chmod 600 "$l_pair_forwarded_stage" || exit 92;
		if [ -e "$l_pair_primary" ]; then
		  l_pair_recovery=$(mktemp "${l_pair_primary%/*}/.zxfer-backup-recovery.XXXXXX") || exit 92;
		  "$l_pair_cat" "$l_pair_primary" >"$l_pair_recovery" && chmod 600 "$l_pair_recovery" || exit 92;
		fi;
		l_pair_signal=0;
		trap 'l_pair_signal=129' HUP;
		trap 'l_pair_signal=130' INT;
		trap 'l_pair_signal=143' TERM;
		[ ! -L "$l_pair_primary" ] && mv -f "$l_pair_stage" "$l_pair_primary" || exit 92;
		l_pair_published=1;
		[ "$l_pair_signal" -eq 0 ] || exit "$l_pair_signal";
		[ ! -L "$l_pair_forwarded" ] && mv -f "$l_pair_forwarded_stage" "$l_pair_forwarded" || exit 92;
		l_pair_published=0;
		[ "$l_pair_signal" -eq 0 ] || exit "$l_pair_signal";
	EOF
}

# Purpose: Render the remote program that validates and prints one metadata
# file: symlink walk (98), missing (94), directory writable by others (91),
# owner (95), mode (96), unknown metadata (97), then cat.
# Usage: zxfer_build_remote_backup_read_cmd PATH HOST
zxfer_build_remote_backup_read_cmd() {
	l_read_cmd_path=$1
	l_read_cmd_host=$2

	case $l_read_cmd_path in
	?*/*) l_read_cmd_parent=${l_read_cmd_path%/*} ;;
	*) l_read_cmd_parent=/ ;;
	esac
	# ls must not read a leading dash as an option.
	l_read_cmd_ls_path=$l_read_cmd_path
	case $l_read_cmd_path in
	-*) l_read_cmd_ls_path=./$l_read_cmd_path ;;
	esac
	case $l_read_cmd_parent in
	-*) l_read_cmd_parent=./$l_read_cmd_parent ;;
	esac
	zxfer_build_remote_backup_script_prelude "$l_read_cmd_host" "$l_read_cmd_path" \
		metadata id ls awk || return "$?"

	zxfer_render_shell_command_from_argv set -- "$l_read_cmd_path" \
		"$l_read_cmd_ls_path" "$l_read_cmd_parent" "${g_cmd_cat:-cat}"
	printf '%s;\n' "$g_zxfer_shell_command_result"
	zxfer_print_backup_script_body <<-'EOF'
		l_read_cmd_path=$1;
		l_read_cmd_ls_path=$2;
		l_read_cmd_parent=$3;
		l_read_cmd_cat=$4;
		if [ ! -f "$l_read_cmd_path" ]; then
		  exit 94;
		fi;
		l_expected_uid=$(id -u 2>/dev/null | awk '/^[0-9]+$/');
		[ "$l_expected_uid" != '' ] || exit 97;
		l_parent_line=$(ls -ldn "$l_read_cmd_parent" 2>/dev/null) || exit 97;
		l_parent_uid=$(printf '%s\n' "$l_parent_line" | awk '$3 ~ /^[0-9]+$/ { print $3 }');
		[ "$l_parent_uid" != '' ] || exit 97;
		if [ "$l_parent_uid" != 0 ] && [ "$l_parent_uid" != "$l_expected_uid" ]; then
		  exit 91;
		fi;
		case "$l_parent_line" in
		?????w* | ????????w*)
		  case "$l_parent_line" in
		  ?????????[tT]*) ;;
		  *) exit 91 ;;
		  esac;
		  ;;
		esac;
		l_file_line=$(ls -ldn "$l_read_cmd_ls_path" 2>/dev/null) || exit 97;
		l_file_uid=$(printf '%s\n' "$l_file_line" | awk '$3 ~ /^[0-9]+$/ { print $3 }');
		[ "$l_file_uid" != '' ] || exit 97;
		if [ "$l_file_uid" != 0 ] && [ "$l_file_uid" != "$l_expected_uid" ]; then
		  exit 95;
		fi;
		case "$l_file_line" in
		-rw-------*) ;;
		*) exit 96 ;;
		esac;
		"$l_read_cmd_cat" "$l_read_cmd_path";
	EOF
}

# Purpose: Render the remote program that prints, relative to
# ZXFER_BACKUP_DIR, every existing storage directory (or symlink) of ROOT,
# its ancestors, and its descendants, skipping the metadata trees.
# Usage: zxfer_build_remote_backup_storage_listing_cmd ROOT HOST; the program
# prints nothing without ZXFER_BACKUP_DIR and exits 92 for a symlinked path
# component, 97 when the listing fails, 99 when find(1) is missing. Only
# ROOT's own storage directory is searched with find, so a store that is
# empty, missing, or has no directory for ROOT needs no find.
zxfer_build_remote_backup_storage_listing_cmd() {
	l_listing_root=$1
	l_listing_host=$2

	# The root and each ancestor, pool first.
	set -- "$l_listing_root"
	l_listing_ancestor=$l_listing_root
	while :; do
		case $l_listing_ancestor in
		*/*) l_listing_ancestor=${l_listing_ancestor%/*} ;;
		*) break ;;
		esac
		set -- "$l_listing_ancestor" "$@"
	done
	zxfer_build_remote_backup_script_prelude "$l_listing_host" \
		"$g_backup_storage_root/$l_listing_root" directory || return "$?"

	zxfer_render_shell_command_from_argv set -- "$g_backup_storage_root" "$l_listing_root" "$@"
	printf '%s;\n' "$g_zxfer_shell_command_result"
	zxfer_print_backup_script_body <<-'EOF'
		l_listing_store=$1;
		l_listing_root=$2;
		shift 2;
		if [ ! -d "$l_listing_store" ]; then
		  exit 0;
		fi;
		cd "$l_listing_store" || exit 97;
		for l_listing_dir in "$@"; do
		  if [ -d "$l_listing_dir" ] || [ -L "$l_listing_dir" ]; then
		    printf '%s\n' "$l_listing_dir";
		  fi;
		done;
		if [ -d "$l_listing_root" ]; then
		  zxfer_require_remote_backup_tool 'find';
		  find "$l_listing_root" -name '.zxfer_backup_info*' -prune -o \( -type d -o -type l \) -print || exit 97;
		fi;
	EOF
}

# Purpose: Render one remote backup program as the ssh pipeline segment shown
# by dry-run output.
# Usage: Collapses the readable program to one physical line (every rendered
# line ends a command) and publishes the prepared ssh command in
# g_zxfer_shell_command_result; failures leave it empty with their status.
zxfer_render_remote_backup_dry_run_shell_command() {
	l_dry_run_host=$1
	l_dry_run_script=$2
	g_zxfer_shell_command_result=""

	l_dry_run_line=""
	while IFS= read -r l_dry_run_script_line || [ -n "$l_dry_run_script_line" ]; do
		[ -n "$l_dry_run_script_line" ] || continue
		l_dry_run_line="${l_dry_run_line:+$l_dry_run_line }$l_dry_run_script_line"
	done <<EOF
$l_dry_run_script
EOF
	[ -n "$l_dry_run_line" ] || return 1
	zxfer_publish_prepared_ssh_shell_command_for_host_or_throw "$l_dry_run_host" \
		"$l_dry_run_line" || return "$?"
}
