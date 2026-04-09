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
# SNAPSHOT RECORD STATE / INDEXES / IDENTITIES
################################################################################

# Module contract:
# owns globals: destination-existence cache and snapshot-record index state.
# reads globals: g_cmd_awk and temp-root helpers.
# mutates caches: in-memory destination-existence state and on-disk snapshot index files.
# returns via stdout: parsed snapshot identities, indexed records, and cache-backed existence probes.

zxfer_reset_destination_existence_cache() {
	g_destination_existence_cache=""
	g_destination_existence_cache_root=""
	g_destination_existence_cache_root_complete=0
}

zxfer_reset_snapshot_record_indexes() {
	if [ -n "${g_zxfer_snapshot_index_dir:-}" ] && [ -d "$g_zxfer_snapshot_index_dir" ]; then
		rm -rf "$g_zxfer_snapshot_index_dir"
	fi

	g_zxfer_snapshot_index_dir=""
	g_zxfer_snapshot_index_unavailable=0
	g_zxfer_source_snapshot_record_index=""
	g_zxfer_source_snapshot_record_index_ready=0
	g_zxfer_destination_snapshot_record_index=""
	g_zxfer_destination_snapshot_record_index_ready=0
}

zxfer_ensure_snapshot_index_dir() {
	if [ "${g_zxfer_snapshot_index_unavailable:-0}" -eq 1 ]; then
		return 1
	fi

	if [ -n "${g_zxfer_snapshot_index_dir:-}" ] && [ -d "$g_zxfer_snapshot_index_dir" ]; then
		return 0
	fi

	if ! l_tmpdir=$(zxfer_try_get_effective_tmpdir); then
		g_zxfer_snapshot_index_unavailable=1
		g_zxfer_snapshot_index_dir=""
		return 1
	fi
	if ! g_zxfer_snapshot_index_dir=$(mktemp -d "$l_tmpdir/zxfer-snapshot-index.XXXXXX" 2>/dev/null); then
		g_zxfer_snapshot_index_unavailable=1
		g_zxfer_snapshot_index_dir=""
		return 1
	fi

	return 0
}

zxfer_build_snapshot_record_index() {
	l_side=$1
	l_snapshot_records=$2

	case "$l_side" in
	source)
		g_zxfer_source_snapshot_record_index=""
		g_zxfer_source_snapshot_record_index_ready=0
		;;
	destination)
		g_zxfer_destination_snapshot_record_index=""
		g_zxfer_destination_snapshot_record_index_ready=0
		;;
	*)
		return 1
		;;
	esac

	if ! zxfer_ensure_snapshot_index_dir; then
		return 1
	fi

	l_side_dir=$g_zxfer_snapshot_index_dir/$l_side
	rm -rf "$l_side_dir"
	if ! mkdir -p "$l_side_dir"; then
		g_zxfer_snapshot_index_unavailable=1
		return 1
	fi

	# shellcheck disable=SC2016  # awk program should see literal $0.
	l_index_map=$(printf '%s\n' "$l_snapshot_records" | "${g_cmd_awk:-awk}" -v index_dir="$l_side_dir" '
$0 != "" {
	record = $0
	tab_pos = index(record, "\t")
	snapshot_path = (tab_pos > 0 ? substr(record, 1, tab_pos - 1) : record)
	at_pos = index(snapshot_path, "@")
	if (at_pos <= 0)
		next
	dataset = substr(snapshot_path, 1, at_pos - 1)
	if (!(dataset in file_paths)) {
		file_count++
		file_paths[dataset] = index_dir "/" file_count ".records"
		dataset_order[file_count] = dataset
	}
	print record >> file_paths[dataset]
	# Avoid exhausting awk output descriptors on deep recursive trees.
	close(file_paths[dataset])
}
END {
	for (i = 1; i <= file_count; i++)
		print dataset_order[i] "\t" file_paths[dataset_order[i]]
}') || {
		g_zxfer_snapshot_index_unavailable=1
		return 1
	}

	case "$l_side" in
	source)
		g_zxfer_source_snapshot_record_index=$l_index_map
		g_zxfer_source_snapshot_record_index_ready=1
		;;
	destination)
		g_zxfer_destination_snapshot_record_index=$l_index_map
		g_zxfer_destination_snapshot_record_index_ready=1
		;;
	esac

	return 0
}

zxfer_ensure_source_snapshot_record_cache() {
	[ -n "${g_lzfs_list_hr_S_snap:-}" ] && return 0
	[ -n "${g_lzfs_list_hr_snap:-}" ] || return 1

	if ! g_lzfs_list_hr_S_snap=$(zxfer_reverse_snapshot_record_list "$g_lzfs_list_hr_snap"); then
		return 1
	fi

	[ -n "${g_lzfs_list_hr_S_snap:-}" ]
}

zxfer_ensure_snapshot_record_index_for_side() {
	l_side=$1

	case "$l_side" in
	source)
		[ "${g_zxfer_source_snapshot_record_index_ready:-0}" -eq 1 ] && return 0
		zxfer_ensure_source_snapshot_record_cache || return 1
		l_snapshot_records=$g_lzfs_list_hr_S_snap
		;;
	destination)
		[ "${g_zxfer_destination_snapshot_record_index_ready:-0}" -eq 1 ] && return 0
		l_snapshot_records=${g_rzfs_list_hr_snap:-}
		;;
	*)
		return 1
		;;
	esac

	[ -n "$l_snapshot_records" ] || return 1

	zxfer_build_snapshot_record_index "$l_side" "$l_snapshot_records"
}

zxfer_get_indexed_snapshot_records_for_dataset() {
	l_side=$1
	l_dataset=$2

	case "$l_side" in
	source)
		l_index_map=${g_zxfer_source_snapshot_record_index:-}
		l_index_ready=${g_zxfer_source_snapshot_record_index_ready:-0}
		;;
	destination)
		l_index_map=${g_zxfer_destination_snapshot_record_index:-}
		l_index_ready=${g_zxfer_destination_snapshot_record_index_ready:-0}
		;;
	*)
		return 1
		;;
	esac

	[ "$l_index_ready" -eq 1 ] || return 1

	while IFS='	' read -r l_indexed_dataset l_record_file || [ -n "${l_indexed_dataset}${l_record_file}" ]; do
		[ -n "$l_indexed_dataset" ] || continue
		if [ "$l_indexed_dataset" = "$l_dataset" ]; then
			if [ -f "$l_record_file" ]; then
				cat "$l_record_file"
				return 0
			fi
			return 1
		fi
	done <<-EOF
		$l_index_map
	EOF

	return 0
}

zxfer_get_snapshot_records_for_dataset() {
	l_side=$1
	l_dataset=$2

	if zxfer_get_indexed_snapshot_records_for_dataset "$l_side" "$l_dataset"; then
		return 0
	fi
	if zxfer_ensure_snapshot_record_index_for_side "$l_side"; then
		if zxfer_get_indexed_snapshot_records_for_dataset "$l_side" "$l_dataset"; then
			return 0
		fi
	fi

	case "$l_side" in
	source)
		zxfer_ensure_source_snapshot_record_cache || return 0
		# shellcheck disable=SC2016  # awk program should see literal $1/$0.
		printf '%s\n' "$g_lzfs_list_hr_S_snap" | "${g_cmd_awk:-awk}" -F@ -v ds="$l_dataset" '$1 == ds { print $0 }'
		;;
	destination)
		# shellcheck disable=SC2016  # awk program should see literal $1/$0.
		printf '%s\n' "$g_rzfs_list_hr_snap" | "${g_cmd_awk:-awk}" -F@ -v ds="$l_dataset" '$1 == ds { print $0 }'
		;;
	*)
		return 1
		;;
	esac
}

zxfer_set_destination_existence_cache_entry() {
	l_dataset=$1
	l_exists_state=$2
	l_updated_cache=""

	while IFS='	' read -r l_cached_dataset l_cached_state || [ -n "${l_cached_dataset}${l_cached_state}" ]; do
		[ -n "$l_cached_dataset" ] || continue
		[ "$l_cached_dataset" = "$l_dataset" ] && continue
		if [ -n "$l_updated_cache" ]; then
			l_updated_cache="$l_updated_cache
$l_cached_dataset	$l_cached_state"
		else
			l_updated_cache="$l_cached_dataset	$l_cached_state"
		fi
	done <<-EOF
		${g_destination_existence_cache:-}
	EOF

	if [ -n "$l_updated_cache" ]; then
		g_destination_existence_cache="$l_updated_cache
$l_dataset	$l_exists_state"
	else
		g_destination_existence_cache="$l_dataset	$l_exists_state"
	fi
}

zxfer_get_destination_existence_cache_entry() {
	l_dataset=$1

	while IFS='	' read -r l_cached_dataset l_cached_state || [ -n "${l_cached_dataset}${l_cached_state}" ]; do
		[ -n "$l_cached_dataset" ] || continue
		if [ "$l_cached_dataset" = "$l_dataset" ]; then
			printf '%s\n' "$l_cached_state"
			return 0
		fi
	done <<-EOF
		${g_destination_existence_cache:-}
	EOF

	if [ "${g_destination_existence_cache_root_complete:-0}" -eq 1 ] &&
		[ -n "${g_destination_existence_cache_root:-}" ]; then
		case "$l_dataset" in
		"$g_destination_existence_cache_root" | "$g_destination_existence_cache_root"/*)
			printf '%s\n' 0
			return 0
			;;
		esac
	fi

	return 1
}

zxfer_seed_destination_existence_cache_from_recursive_list() {
	l_root_dataset=$1
	l_recursive_dest_list=$2

	zxfer_reset_destination_existence_cache
	g_destination_existence_cache_root=$l_root_dataset
	g_destination_existence_cache_root_complete=1

	while IFS= read -r l_dataset; do
		[ -n "$l_dataset" ] || continue
		zxfer_set_destination_existence_cache_entry "$l_dataset" 1
	done <<-EOF
		$l_recursive_dest_list
	EOF
}

zxfer_mark_destination_root_missing_in_cache() {
	l_root_dataset=$1

	zxfer_reset_destination_existence_cache
	g_destination_existence_cache_root=$l_root_dataset
	g_destination_existence_cache_root_complete=1
	[ -n "$l_root_dataset" ] && zxfer_set_destination_existence_cache_entry "$l_root_dataset" 0
}

zxfer_mark_destination_hierarchy_exists() {
	l_dataset=$1
	l_cache_root=${g_destination_existence_cache_root:-}

	while [ -n "$l_dataset" ]; do
		zxfer_set_destination_existence_cache_entry "$l_dataset" 1
		if [ -n "$l_cache_root" ] && [ "$l_dataset" = "$l_cache_root" ]; then
			break
		fi
		l_parent_dataset=${l_dataset%/*}
		[ "$l_parent_dataset" = "$l_dataset" ] && break
		l_dataset=$l_parent_dataset
	done
}

zxfer_note_destination_dataset_exists() {
	l_dataset=$1
	l_created_dataset=$1
	l_recursive_dest_list=${g_recursive_dest_list:-}

	[ -n "$l_dataset" ] || return

	zxfer_mark_destination_hierarchy_exists "$l_dataset"

	case "
$l_recursive_dest_list
" in
	*"
$l_created_dataset
"*) ;;
	*)
		if [ -n "$l_recursive_dest_list" ]; then
			g_recursive_dest_list="$g_recursive_dest_list
$l_created_dataset"
		else
			g_recursive_dest_list=$l_created_dataset
		fi
		;;
	esac
}

zxfer_extract_snapshot_path() {
	l_snapshot_record=$1
	l_tab='	'

	case "$l_snapshot_record" in
	*"$l_tab"*)
		printf '%s\n' "${l_snapshot_record%%	*}"
		;;
	*)
		printf '%s\n' "$l_snapshot_record"
		;;
	esac
}

# Function to extract snapshot name
zxfer_extract_snapshot_name() {
	l_snapshot_path=$(zxfer_extract_snapshot_path "$1")

	case "$l_snapshot_path" in
	*@*)
		printf '%s\n' "${l_snapshot_path#*@}"
		;;
	*)
		printf '%s\n' ""
		;;
	esac
}

zxfer_extract_snapshot_dataset() {
	l_snapshot_path=$(zxfer_extract_snapshot_path "$1")

	case "$l_snapshot_path" in
	*@*)
		printf '%s\n' "${l_snapshot_path%@*}"
		;;
	*)
		printf '%s\n' ""
		;;
	esac
}

zxfer_extract_snapshot_guid() {
	l_snapshot_record=$1
	l_tab='	'

	case "$l_snapshot_record" in
	*"$l_tab"*)
		printf '%s\n' "${l_snapshot_record#*	}"
		;;
	*)
		printf '%s\n' ""
		;;
	esac
}

zxfer_extract_snapshot_identity() {
	l_snapshot_name=$(zxfer_extract_snapshot_name "$1")
	l_snapshot_guid=$(zxfer_extract_snapshot_guid "$1")

	[ -n "$l_snapshot_name" ] || {
		printf '%s\n' ""
		return
	}

	if [ -n "$l_snapshot_guid" ]; then
		printf '%s\t%s\n' "$l_snapshot_name" "$l_snapshot_guid"
	else
		printf '%s\n' "$l_snapshot_name"
	fi
}

zxfer_normalize_snapshot_record_list() {
	printf '%s\n' "$1" | tr ' ' '\n'
}

zxfer_snapshot_record_list_contains_guid() {
	l_tab='	'
	case "$1" in
	*"$l_tab"*)
		return 0
		;;
	esac

	return 1
}

zxfer_reverse_snapshot_record_list() {
	l_snapshot_records=$1

	[ -n "$l_snapshot_records" ] || return 0

	# shellcheck disable=SC2016  # awk program should see literal $0/NR.
	printf '%s\n' "$l_snapshot_records" | "${g_cmd_awk:-awk}" '{ l_records[NR] = $0 } END { for (l_i = NR; l_i >= 1; l_i--) if (l_records[l_i] != "") print l_records[l_i] }'
}

zxfer_snapshot_record_lists_share_snapshot_name() {
	l_source_records=$1
	l_destination_records=$2
	l_section_break="@@ZXFER_SNAPSHOT_NAME_SET_BREAK@@"
	l_overlap_awk=$(
		cat <<'EOF'
BEGIN { in_source = 0 }
$0 == section_break {
	in_source = 1
	next
}
!in_source {
	if ($0 != "") {
		record = $0
		tab_pos = index(record, "\t")
		snapshot_path = (tab_pos > 0 ? substr(record, 1, tab_pos - 1) : record)
		at_pos = index(snapshot_path, "@")
		if (at_pos > 0)
			destination_names[substr(snapshot_path, at_pos + 1)] = 1
	}
	next
}
$0 != "" {
	record = $0
	tab_pos = index(record, "\t")
	snapshot_path = (tab_pos > 0 ? substr(record, 1, tab_pos - 1) : record)
	at_pos = index(snapshot_path, "@")
	if (at_pos > 0) {
		snapshot_name = substr(snapshot_path, at_pos + 1)
		if (snapshot_name in destination_names)
			found = 1
	}
}
END { exit(found ? 0 : 1) }
EOF
	)

	{
		zxfer_normalize_snapshot_record_list "$l_destination_records"
		printf '%s\n' "$l_section_break"
		zxfer_normalize_snapshot_record_list "$l_source_records"
	} | "${g_cmd_awk:-awk}" -v section_break="$l_section_break" "$l_overlap_awk"
}

zxfer_filter_snapshot_identity_records_to_reference_paths() {
	l_identity_records=$1
	l_reference_records=$2
	l_section_break="@@ZXFER_SNAPSHOT_PATH_FILTER_BREAK@@"
	l_filter_awk=$(
		cat <<'EOF'
BEGIN { in_identity = 0 }
$0 == section_break {
	in_identity = 1
	next
}
!in_identity {
	if ($0 != "") {
		record = $0
		tab_pos = index(record, "\t")
		snapshot_path = (tab_pos > 0 ? substr(record, 1, tab_pos - 1) : record)
		reference_paths[snapshot_path] = 1
	}
	next
}
$0 != "" {
	record = $0
	tab_pos = index(record, "\t")
	snapshot_path = (tab_pos > 0 ? substr(record, 1, tab_pos - 1) : record)
	if (snapshot_path in reference_paths)
		print record
}
EOF
	)

	{
		zxfer_normalize_snapshot_record_list "$l_reference_records"
		printf '%s\n' "$l_section_break"
		zxfer_normalize_snapshot_record_list "$l_identity_records"
	} | "${g_cmd_awk:-awk}" -v section_break="$l_section_break" "$l_filter_awk"
}

zxfer_get_source_snapshot_identity_records_for_dataset() {
	l_dataset=$1

	if ! l_snapshot_records=$(zxfer_run_source_zfs_cmd list -H -o name,guid -s creation -d 1 -t snapshot "$l_dataset"); then
		return 1
	fi

	l_snapshot_records=$(zxfer_normalize_snapshot_record_list "$l_snapshot_records")
	zxfer_reverse_snapshot_record_list "$l_snapshot_records"
}

zxfer_get_destination_snapshot_identity_records_for_dataset() {
	l_dataset=$1

	if ! l_snapshot_records=$(zxfer_run_destination_zfs_cmd list -Hr -o name,guid -t snapshot "$l_dataset"); then
		return 1
	fi

	while IFS= read -r l_snapshot_record; do
		[ -n "$l_snapshot_record" ] || continue
		l_snapshot_path=$(zxfer_extract_snapshot_path "$l_snapshot_record")
		case "$l_snapshot_path" in
		"$l_dataset"@*)
			printf '%s\n' "$l_snapshot_record"
			;;
		esac
	done <<-EOF
		$(zxfer_normalize_snapshot_record_list "$l_snapshot_records")
	EOF
}

zxfer_get_snapshot_identity_records_for_dataset() {
	l_side=$1
	l_dataset=$2
	l_reference_records=${3:-}

	case "$l_side" in
	source)
		l_identity_records=$(zxfer_get_source_snapshot_identity_records_for_dataset "$l_dataset") || return 1
		;;
	destination)
		l_identity_records=$(zxfer_get_destination_snapshot_identity_records_for_dataset "$l_dataset") || return 1
		;;
	*)
		return 1
		;;
	esac

	if [ -n "$l_reference_records" ]; then
		zxfer_filter_snapshot_identity_records_to_reference_paths "$l_identity_records" "$l_reference_records"
	else
		printf '%s\n' "$l_identity_records"
	fi
}

#
# Initializes OS and local/remote specific variables
#
