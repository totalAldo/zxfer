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

ZXFER_SNAPSHOT_RECORD_INDEX_OBJECT_KIND="snapshot-record-index"
ZXFER_SNAPSHOT_RECORDS_OBJECT_KIND="snapshot-records"

# Purpose: Reset the destination existence cache so the next snapshot-state
# pass starts from a clean state.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks before this module reuses mutable scratch globals or cached decisions.
zxfer_reset_destination_existence_cache() {
	g_destination_existence_cache=""
	g_destination_existence_cache_root=""
	g_destination_existence_cache_root_complete=0
}

# Purpose: Reset the snapshot record indexes so the next snapshot-state pass
# starts from a clean state.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks before this module reuses mutable scratch globals or cached decisions.
zxfer_reset_snapshot_record_indexes() {
	if [ -n "${g_zxfer_snapshot_index_dir:-}" ] && [ -d "$g_zxfer_snapshot_index_dir" ]; then
		zxfer_cleanup_runtime_artifact_path "$g_zxfer_snapshot_index_dir"
	fi

	g_zxfer_snapshot_index_dir=""
	g_zxfer_snapshot_index_unavailable=0
	g_zxfer_source_snapshot_record_index_dir=""
	g_zxfer_source_snapshot_record_index=""
	g_zxfer_source_snapshot_record_index_ready=0
	g_zxfer_destination_snapshot_record_index_dir=""
	g_zxfer_destination_snapshot_record_index=""
	g_zxfer_destination_snapshot_record_index_ready=0
}

# Purpose: Invalidate destination snapshot-record caches after destination
# snapshot state changes.
# Usage: Called after receives, snapshot destroys, and rollbacks so later
# planning cannot reuse stale destination snapshot lists or indexes.
zxfer_invalidate_destination_snapshot_record_cache() {
	if [ -n "${g_zxfer_destination_snapshot_record_cache_file:-}" ]; then
		zxfer_cleanup_runtime_artifact_path "$g_zxfer_destination_snapshot_record_cache_file"
	fi

	g_rzfs_list_hr_snap=""
	g_zxfer_destination_snapshot_record_cache_file=""
	zxfer_clear_snapshot_record_index_state_for_side destination
	if command -v zxfer_reset_destination_snapshot_creation_cache >/dev/null 2>&1; then
		zxfer_reset_destination_snapshot_creation_cache
	fi
}

# Purpose: Ensure the snapshot index directory exists and is ready before the
# flow continues.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks before later helpers assume the resource or cache is available.
zxfer_ensure_snapshot_index_dir() {
	if [ "${g_zxfer_snapshot_index_unavailable:-0}" -eq 1 ]; then
		return 1
	fi

	if [ -n "${g_zxfer_snapshot_index_dir:-}" ] && [ -d "$g_zxfer_snapshot_index_dir" ]; then
		return 0
	fi

	l_index_status=0
	zxfer_create_runtime_artifact_dir "zxfer-snapshot-index" >/dev/null ||
		l_index_status=$?
	if [ "$l_index_status" -ne 0 ]; then
		g_zxfer_snapshot_index_unavailable=1
		g_zxfer_snapshot_index_dir=""
		return "$l_index_status"
	fi
	g_zxfer_snapshot_index_dir=$g_zxfer_runtime_artifact_path_result

	return 0
}

# Purpose: Clear the snapshot record index state for side from the module-owned
# state.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when later helpers must not see an old cached or role-specific value.
zxfer_clear_snapshot_record_index_state_for_side() {
	l_side=$1
	l_remove_dir=${2:-1}

	case "$l_side" in
	source)
		if [ "$l_remove_dir" -eq 1 ] &&
			[ -n "${g_zxfer_source_snapshot_record_index_dir:-}" ] &&
			[ -d "$g_zxfer_source_snapshot_record_index_dir" ]; then
			zxfer_cleanup_runtime_artifact_path "$g_zxfer_source_snapshot_record_index_dir"
		fi
		g_zxfer_source_snapshot_record_index_dir=""
		g_zxfer_source_snapshot_record_index=""
		g_zxfer_source_snapshot_record_index_ready=0
		;;
	destination)
		if [ "$l_remove_dir" -eq 1 ] &&
			[ -n "${g_zxfer_destination_snapshot_record_index_dir:-}" ] &&
			[ -d "$g_zxfer_destination_snapshot_record_index_dir" ]; then
			zxfer_cleanup_runtime_artifact_path "$g_zxfer_destination_snapshot_record_index_dir"
		fi
		g_zxfer_destination_snapshot_record_index_dir=""
		g_zxfer_destination_snapshot_record_index=""
		g_zxfer_destination_snapshot_record_index_ready=0
		;;
	*)
		return 1
		;;
	esac

	return 0
}

# Purpose: Update the snapshot record index state for side in the shared
# runtime state.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks after a probe or planning step changes the active context that later
# helpers should use.
zxfer_set_snapshot_record_index_state_for_side() {
	l_side=$1
	l_index_dir=$2
	l_index_map=$3

	case "$l_side" in
	source)
		g_zxfer_source_snapshot_record_index_dir=$l_index_dir
		g_zxfer_source_snapshot_record_index=$l_index_map
		g_zxfer_source_snapshot_record_index_ready=1
		;;
	destination)
		g_zxfer_destination_snapshot_record_index_dir=$l_index_dir
		g_zxfer_destination_snapshot_record_index=$l_index_map
		g_zxfer_destination_snapshot_record_index_ready=1
		;;
	*)
		return 1
		;;
	esac

	return 0
}

# Purpose: Validate the snapshot record index manifest file before zxfer relies
# on it.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks to fail closed on malformed, unsafe, or stale input.
zxfer_validate_snapshot_record_index_manifest_file() {
	l_index_dir=$1
	l_manifest_path=$2
	l_entry_count_expected=$3
	l_index_map=""
	l_seen_datasets=""
	l_seen_relpaths=""
	l_entry_count=0

	[ -f "$l_manifest_path" ] || return 1
	[ ! -L "$l_manifest_path" ] || return 1
	[ ! -h "$l_manifest_path" ] || return 1
	[ -r "$l_manifest_path" ] || return 1

	if zxfer_read_runtime_artifact_file "$l_manifest_path" >/dev/null; then
		l_manifest_contents=$g_zxfer_runtime_artifact_read_result
	else
		l_read_status=$?
		return "$l_read_status"
	fi
	case "$l_manifest_contents" in
	*'
')
		l_manifest_contents=${l_manifest_contents%?}
		;;
	esac

	while IFS='	' read -r l_dataset l_record_relpath l_extra || [ -n "${l_dataset}${l_record_relpath}${l_extra}" ]; do
		[ -n "$l_dataset" ] || return 1
		[ -n "$l_record_relpath" ] || return 1
		[ -z "$l_extra" ] || return 1
		case "
$l_seen_datasets
" in
		*"
$l_dataset
"*)
			return 1
			;;
		esac
		case "
$l_seen_relpaths
" in
		*"
$l_record_relpath
"*)
			return 1
			;;
		esac
		case "$l_record_relpath" in
		records/*) ;;
		/* | ../* | */../* | */.. | [.] | [.]/* | */[.]/* | */[.])
			return 1
			;;
		*)
			return 1
			;;
		esac

		l_record_path="$l_index_dir/$l_record_relpath"
		l_manifest_status=0
		zxfer_read_cache_object_file \
			"$l_record_path" "$ZXFER_SNAPSHOT_RECORDS_OBJECT_KIND" >/dev/null ||
			l_manifest_status=$?
		if [ "$l_manifest_status" -ne 0 ]; then
			return "$l_manifest_status"
		fi

		if [ -n "$l_index_map" ]; then
			l_index_map="$l_index_map
$l_dataset	$l_record_path"
		else
			l_index_map="$l_dataset	$l_record_path"
		fi
		if [ -n "$l_seen_datasets" ]; then
			l_seen_datasets="$l_seen_datasets
$l_dataset"
		else
			l_seen_datasets=$l_dataset
		fi
		if [ -n "$l_seen_relpaths" ]; then
			l_seen_relpaths="$l_seen_relpaths
$l_record_relpath"
		else
			l_seen_relpaths=$l_record_relpath
		fi
		l_entry_count=$((l_entry_count + 1))
	done <<EOF
$l_manifest_contents
EOF

	[ "$l_entry_count" -eq "$l_entry_count_expected" ] || return 1
	[ "$l_entry_count" -gt 0 ] || return 1

	printf '%s\n' "$l_index_map"
}

# Purpose: Validate the snapshot record index object directory before zxfer
# relies on it.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks to fail closed on malformed, unsafe, or stale input.
zxfer_validate_snapshot_record_index_object_dir() {
	l_index_dir=$1
	l_expected_side=$2
	l_meta_path="$l_index_dir/meta"
	l_manifest_path="$l_index_dir/manifest.tsv"

	[ -d "$l_index_dir" ] || return 1
	[ ! -L "$l_index_dir" ] || return 1
	[ ! -h "$l_index_dir" ] || return 1
	l_index_object_status=0
	zxfer_read_cache_object_file \
		"$l_meta_path" "$ZXFER_SNAPSHOT_RECORD_INDEX_OBJECT_KIND" >/dev/null ||
		l_index_object_status=$?
	if [ "$l_index_object_status" -ne 0 ]; then
		return "$l_index_object_status"
	fi
	[ "$g_zxfer_cache_object_payload_result" = "ready" ] || return 1
	l_index_object_status=0
	l_manifest_name=$(zxfer_get_cache_object_metadata_value \
		"$g_zxfer_cache_object_metadata_result" manifest) ||
		l_index_object_status=$?
	if [ "$l_index_object_status" -ne 0 ]; then
		return "$l_index_object_status"
	fi
	[ "$l_manifest_name" = "manifest.tsv" ] || return 1
	l_index_object_status=0
	l_entry_count_expected=$(zxfer_get_cache_object_metadata_value \
		"$g_zxfer_cache_object_metadata_result" entries) ||
		l_index_object_status=$?
	if [ "$l_index_object_status" -ne 0 ]; then
		return "$l_index_object_status"
	fi
	case "$l_entry_count_expected" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	l_index_object_status=0
	l_meta_side=$(zxfer_get_cache_object_metadata_value \
		"$g_zxfer_cache_object_metadata_result" side) ||
		l_index_object_status=$?
	if [ "$l_index_object_status" -ne 0 ]; then
		return "$l_index_object_status"
	fi
	[ -z "$l_expected_side" ] || [ "$l_meta_side" = "$l_expected_side" ] || return 1

	zxfer_validate_snapshot_record_index_manifest_file \
		"$l_index_dir" "$l_manifest_path" "$l_entry_count_expected"
}

# Purpose: Validate the snapshot record index for side before zxfer relies on
# it.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks to fail closed on malformed, unsafe, or stale input.
zxfer_validate_snapshot_record_index_for_side() {
	l_side=$1

	case "$l_side" in
	source)
		l_index_dir=${g_zxfer_source_snapshot_record_index_dir:-}
		l_index_ready=${g_zxfer_source_snapshot_record_index_ready:-0}
		;;
	destination)
		l_index_dir=${g_zxfer_destination_snapshot_record_index_dir:-}
		l_index_ready=${g_zxfer_destination_snapshot_record_index_ready:-0}
		;;
	*)
		return 1
		;;
	esac

	[ "$l_index_ready" -eq 1 ] || return 1
	[ -n "$l_index_dir" ] || {
		zxfer_clear_snapshot_record_index_state_for_side "$l_side" 0
		return 1
	}

	l_validate_status=0
	l_index_map=$(zxfer_validate_snapshot_record_index_object_dir "$l_index_dir" "$l_side") ||
		l_validate_status=$?
	if [ "$l_validate_status" -ne 0 ]; then
		zxfer_clear_snapshot_record_index_state_for_side "$l_side"
		return "$l_validate_status"
	fi

	zxfer_set_snapshot_record_index_state_for_side "$l_side" "$l_index_dir" "$l_index_map"
}

# Purpose: Return the snapshot-record cache file path for the requested source
# or destination side.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks before snapshot-record cache data is read or rewritten for one side of
# the run.
zxfer_snapshot_record_cache_file_for_side() {
	case "$1" in
	source)
		printf '%s\n' "${g_zxfer_source_snapshot_record_cache_file:-}"
		;;
	destination)
		printf '%s\n' "${g_zxfer_destination_snapshot_record_cache_file:-}"
		;;
	*)
		return 1
		;;
	esac
}

# Purpose: Build the snapshot record index from either a file or in-memory
# records.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks so public builders share one validation, publish, and cleanup path.
zxfer_build_snapshot_record_index_core() {
	l_side=$1
	l_input_kind=$2
	l_snapshot_records_input=$3
	l_stage_dir=""
	l_manifest_path=""
	l_records_dir=""
	l_stage_map=""
	l_manifest_payload=""

	case "$l_side" in
	source)
		l_previous_index_dir=${g_zxfer_source_snapshot_record_index_dir:-}
		;;
	destination)
		l_previous_index_dir=${g_zxfer_destination_snapshot_record_index_dir:-}
		;;
	*)
		return 1
		;;
	esac
	case "$l_input_kind" in
	file)
		[ -r "$l_snapshot_records_input" ] || return 1
		;;
	records)
		:
		;;
	*)
		return 1
		;;
	esac

	l_build_status=0
	zxfer_ensure_snapshot_index_dir || l_build_status=$?
	if [ "$l_build_status" -ne 0 ]; then
		return "$l_build_status"
	fi

	l_build_status=0
	zxfer_create_cache_object_stage_dir_in_parent \
		"$g_zxfer_snapshot_index_dir" "zxfer-snapshot-index-$l_side" >/dev/null ||
		l_build_status=$?
	if [ "$l_build_status" -ne 0 ]; then
		return "$l_build_status"
	fi
	l_stage_dir=$g_zxfer_runtime_artifact_path_result
	l_manifest_path="$l_stage_dir/manifest.tsv"
	l_records_dir="$l_stage_dir/records"
	l_build_status=0
	mkdir -p "$l_records_dir" || l_build_status=$?
	if [ "$l_build_status" -ne 0 ]; then
		zxfer_cleanup_cache_object_stage_dir "$l_stage_dir"
		return "$l_build_status"
	fi

	l_build_status=0
	# shellcheck disable=SC2016  # awk program should see literal $0.
	l_snapshot_index_stage_awk='
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
		relative_paths[dataset] = "records/" file_count ".raw"
		file_paths[dataset] = index_dir "/" relative_paths[dataset]
		dataset_order[file_count] = dataset
	}
	print record >> file_paths[dataset]
	# Avoid exhausting awk output descriptors on deep recursive trees.
	close(file_paths[dataset])
}
END {
	for (i = 1; i <= file_count; i++)
		print dataset_order[i] "\t" "records/" i ".records" "\t" file_paths[dataset_order[i]]
}'
	case "$l_input_kind" in
	file)
		l_stage_map=$("${g_cmd_awk:-awk}" -v index_dir="$l_stage_dir" \
			"$l_snapshot_index_stage_awk" "$l_snapshot_records_input") ||
			l_build_status=$?
		;;
	records)
		l_stage_map=$(printf '%s\n' "$l_snapshot_records_input" |
			"${g_cmd_awk:-awk}" -v index_dir="$l_stage_dir" "$l_snapshot_index_stage_awk") ||
			l_build_status=$?
		;;
	esac
	if [ "$l_build_status" -ne 0 ]; then
		zxfer_cleanup_cache_object_stage_dir "$l_stage_dir"
		return "$l_build_status"
	fi

	while IFS='	' read -r l_dataset l_record_relpath l_raw_record_path || [ -n "${l_dataset}${l_record_relpath}${l_raw_record_path}" ]; do
		[ -n "$l_dataset" ] || continue
		[ -n "$l_record_relpath" ] || {
			zxfer_cleanup_cache_object_stage_dir "$l_stage_dir"
			return 1
		}
		[ -n "$l_raw_record_path" ] || {
			zxfer_cleanup_cache_object_stage_dir "$l_stage_dir"
			return 1
		}
		if zxfer_read_runtime_artifact_file "$l_raw_record_path" >/dev/null 2>&1; then
			l_record_payload=$g_zxfer_runtime_artifact_read_result
		else
			l_read_status=$?
			zxfer_cleanup_cache_object_stage_dir "$l_stage_dir"
			return "$l_read_status"
		fi
		l_build_status=0
		zxfer_write_cache_object_contents_to_path \
			"$l_stage_dir/$l_record_relpath" \
			"$ZXFER_SNAPSHOT_RECORDS_OBJECT_KIND" "" "$l_record_payload" ||
			l_build_status=$?
		if [ "$l_build_status" -ne 0 ]; then
			zxfer_cleanup_cache_object_stage_dir "$l_stage_dir"
			return "$l_build_status"
		fi
		l_build_status=0
		zxfer_read_cache_object_file \
			"$l_stage_dir/$l_record_relpath" \
			"$ZXFER_SNAPSHOT_RECORDS_OBJECT_KIND" >/dev/null ||
			l_build_status=$?
		if [ "$l_build_status" -ne 0 ]; then
			zxfer_cleanup_cache_object_stage_dir "$l_stage_dir"
			return "$l_build_status"
		fi
		zxfer_cleanup_runtime_artifact_path "$l_raw_record_path"
		if [ -n "$l_manifest_payload" ]; then
			l_manifest_payload="$l_manifest_payload
$l_dataset	$l_record_relpath"
		else
			l_manifest_payload="$l_dataset	$l_record_relpath"
		fi
	done <<-EOF
		$l_stage_map
	EOF

	[ -n "$l_manifest_payload" ] || {
		zxfer_cleanup_cache_object_stage_dir "$l_stage_dir"
		return 1
	}
	l_build_status=0
	printf '%s\n' "$l_manifest_payload" >"$l_manifest_path" || l_build_status=$?
	if [ "$l_build_status" -ne 0 ]; then
		zxfer_cleanup_cache_object_stage_dir "$l_stage_dir"
		return "$l_build_status"
	fi
	l_build_status=0
	l_stage_entry_count=$(printf '%s\n' "$l_stage_map" | "${g_cmd_awk:-awk}" 'END {print NR + 0}') ||
		l_build_status=$?
	if [ "$l_build_status" -ne 0 ]; then
		zxfer_cleanup_cache_object_stage_dir "$l_stage_dir"
		return "$l_build_status"
	fi
	l_build_status=0
	l_index_map=$(zxfer_validate_snapshot_record_index_manifest_file \
		"$l_stage_dir" "$l_manifest_path" "$l_stage_entry_count") ||
		l_build_status=$?
	if [ "$l_build_status" -ne 0 ]; then
		zxfer_cleanup_cache_object_stage_dir "$l_stage_dir"
		return "$l_build_status"
	fi
	l_build_status=0
	zxfer_write_cache_object_contents_to_path \
		"$l_stage_dir/meta" \
		"$ZXFER_SNAPSHOT_RECORD_INDEX_OBJECT_KIND" \
		"side=$l_side
manifest=manifest.tsv
entries=$l_stage_entry_count" \
		"ready" ||
		l_build_status=$?
	if [ "$l_build_status" -ne 0 ]; then
		zxfer_cleanup_cache_object_stage_dir "$l_stage_dir"
		return "$l_build_status"
	fi
	l_build_status=0
	l_index_map=$(zxfer_validate_snapshot_record_index_object_dir "$l_stage_dir" "$l_side") ||
		l_build_status=$?
	if [ "$l_build_status" -ne 0 ]; then
		zxfer_cleanup_cache_object_stage_dir "$l_stage_dir"
		return "$l_build_status"
	fi

	l_stage_base=${l_stage_dir##*/}
	l_generation_token=${l_stage_base##*.}
	l_published_index_dir="$g_zxfer_snapshot_index_dir/$l_side.$l_generation_token.obj"
	l_build_status=0
	zxfer_publish_cache_object_directory "$l_stage_dir" "$l_published_index_dir" ||
		l_build_status=$?
	if [ "$l_build_status" -ne 0 ]; then
		zxfer_cleanup_cache_object_stage_dir "$l_stage_dir"
		return "$l_build_status"
	fi

	l_build_status=0
	zxfer_set_snapshot_record_index_state_for_side \
		"$l_side" "$l_published_index_dir" "$l_index_map" ||
		l_build_status=$?
	if [ "$l_build_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_published_index_dir"
		return "$l_build_status"
	fi
	if [ -n "$l_previous_index_dir" ] && [ "$l_previous_index_dir" != "$l_published_index_dir" ]; then
		zxfer_cleanup_runtime_artifact_path "$l_previous_index_dir"
	fi

	return 0
}

# Purpose: Build the snapshot record index from file for the next execution or
# comparison step.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks before other helpers consume the assembled value.
zxfer_build_snapshot_record_index_from_file() {
	l_side=$1
	l_snapshot_records_file=$2

	zxfer_build_snapshot_record_index_core "$l_side" file "$l_snapshot_records_file"
}

# Purpose: Build the snapshot record index for the next execution or comparison
# step.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks before other helpers consume the assembled value.
zxfer_build_snapshot_record_index() {
	l_side=$1
	l_snapshot_records=$2

	zxfer_build_snapshot_record_index_core "$l_side" records "$l_snapshot_records"
}

# Purpose: Ensure the source snapshot record cache exists and is ready before
# the flow continues.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks before later helpers assume the resource or cache is available.
zxfer_ensure_source_snapshot_record_cache() {
	[ -n "${g_lzfs_list_hr_S_snap:-}" ] && return 0
	[ -n "${g_lzfs_list_hr_snap:-}" ] || return 1

	if g_lzfs_list_hr_S_snap=$(zxfer_reverse_snapshot_record_list "$g_lzfs_list_hr_snap"); then
		:
	else
		l_status=$?
		return "$l_status"
	fi

	[ -n "${g_lzfs_list_hr_S_snap:-}" ]
}

# Purpose: Ensure the snapshot record index for side exists and is ready before
# the flow continues.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks before later helpers assume the resource or cache is available.
zxfer_ensure_snapshot_record_index_for_side() {
	l_side=$1

	case "$l_side" in
	source)
		if [ "${g_zxfer_source_snapshot_record_index_ready:-0}" -eq 1 ] &&
			zxfer_validate_snapshot_record_index_for_side "$l_side"; then
			return 0
		fi
		if l_snapshot_record_cache_file=$(zxfer_snapshot_record_cache_file_for_side "$l_side") &&
			[ -r "$l_snapshot_record_cache_file" ]; then
			l_status=0
			zxfer_build_snapshot_record_index_from_file "$l_side" "$l_snapshot_record_cache_file" ||
				l_status=$?
			return "$l_status"
		fi
		if zxfer_ensure_source_snapshot_record_cache; then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		l_snapshot_records=$g_lzfs_list_hr_S_snap
		;;
	destination)
		if [ "${g_zxfer_destination_snapshot_record_index_ready:-0}" -eq 1 ] &&
			zxfer_validate_snapshot_record_index_for_side "$l_side"; then
			return 0
		fi
		if l_snapshot_record_cache_file=$(zxfer_snapshot_record_cache_file_for_side "$l_side") &&
			[ -r "$l_snapshot_record_cache_file" ]; then
			l_status=0
			zxfer_build_snapshot_record_index_from_file "$l_side" "$l_snapshot_record_cache_file" ||
				l_status=$?
			return "$l_status"
		fi
		l_snapshot_records=${g_rzfs_list_hr_snap:-}
		;;
	*)
		return 1
		;;
	esac

	[ -n "$l_snapshot_records" ] || return 1

	zxfer_build_snapshot_record_index "$l_side" "$l_snapshot_records"
}

# Purpose: Filter the snapshot record file for dataset down to the subset later
# helpers should act on.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks before reconciliation, execution, or reporting consumes the reduced
# set.
zxfer_filter_snapshot_record_file_for_dataset() {
	l_snapshot_records_file=$1
	l_dataset=$2

	[ -r "$l_snapshot_records_file" ] || return 1

	# shellcheck disable=SC2016  # awk program should see literal $1/$0.
	"${g_cmd_awk:-awk}" -F@ -v ds="$l_dataset" '$1 == ds { print $0 }' "$l_snapshot_records_file"
}

# Purpose: Return the indexed snapshot records for dataset in the form expected
# by later helpers.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_indexed_snapshot_records_for_dataset() {
	l_side=$1
	l_dataset=$2

	case "$l_side" in
	source)
		l_index_dir=${g_zxfer_source_snapshot_record_index_dir:-}
		l_index_map=${g_zxfer_source_snapshot_record_index:-}
		l_index_ready=${g_zxfer_source_snapshot_record_index_ready:-0}
		;;
	destination)
		l_index_dir=${g_zxfer_destination_snapshot_record_index_dir:-}
		l_index_map=${g_zxfer_destination_snapshot_record_index:-}
		l_index_ready=${g_zxfer_destination_snapshot_record_index_ready:-0}
		;;
	*)
		return 1
		;;
	esac

	[ "$l_index_ready" -eq 1 ] || return 1
	l_lookup_status=0
	l_index_map=$(zxfer_validate_snapshot_record_index_object_dir "$l_index_dir" "$l_side") ||
		l_lookup_status=$?
	if [ "$l_lookup_status" -ne 0 ]; then
		zxfer_clear_snapshot_record_index_state_for_side "$l_side"
		return "$l_lookup_status"
	fi
	l_lookup_status=0
	zxfer_set_snapshot_record_index_state_for_side "$l_side" "$l_index_dir" "$l_index_map" ||
		l_lookup_status=$?
	if [ "$l_lookup_status" -ne 0 ]; then
		return "$l_lookup_status"
	fi

	while IFS='	' read -r l_indexed_dataset l_record_file || [ -n "${l_indexed_dataset}${l_record_file}" ]; do
		[ -n "$l_indexed_dataset" ] || continue
		if [ "$l_indexed_dataset" = "$l_dataset" ]; then
			l_lookup_status=0
			zxfer_read_cache_object_file \
				"$l_record_file" "$ZXFER_SNAPSHOT_RECORDS_OBJECT_KIND" >/dev/null ||
				l_lookup_status=$?
			if [ "$l_lookup_status" -eq 0 ]; then
				printf '%s\n' "$g_zxfer_cache_object_payload_result"
				return 0
			fi
			zxfer_clear_snapshot_record_index_state_for_side "$l_side"
			return "$l_lookup_status"
		fi
	done <<-EOF
		$l_index_map
	EOF

	return 0
}

# Purpose: Return the snapshot records for dataset in the form expected by
# later helpers.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_snapshot_records_for_dataset() {
	l_snapshot_lookup_side=$1
	l_snapshot_lookup_dataset=$2

	if zxfer_get_indexed_snapshot_records_for_dataset \
		"$l_snapshot_lookup_side" "$l_snapshot_lookup_dataset"; then
		return 0
	fi
	# The discovery phase already stages whole-tree snapshot cache files. For
	# no-op/delete-only runs, direct filtering avoids building thousands of
	# small index cache objects before any dataset mutation is needed.
	if l_snapshot_record_cache_file=$(zxfer_snapshot_record_cache_file_for_side "$l_snapshot_lookup_side") &&
		[ -r "$l_snapshot_record_cache_file" ]; then
		l_status=0
		zxfer_filter_snapshot_record_file_for_dataset \
			"$l_snapshot_record_cache_file" "$l_snapshot_lookup_dataset" ||
			l_status=$?
		return "$l_status"
	fi

	case "$l_snapshot_lookup_side" in
	source)
		if zxfer_ensure_source_snapshot_record_cache; then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		# shellcheck disable=SC2016  # awk program should see literal $1/$0.
		printf '%s\n' "$g_lzfs_list_hr_S_snap" | "${g_cmd_awk:-awk}" -F@ -v ds="$l_snapshot_lookup_dataset" '$1 == ds { print $0 }'
		;;
	destination)
		# shellcheck disable=SC2016  # awk program should see literal $1/$0.
		printf '%s\n' "$g_rzfs_list_hr_snap" | "${g_cmd_awk:-awk}" -F@ -v ds="$l_snapshot_lookup_dataset" '$1 == ds { print $0 }'
		;;
	*)
		return 1
		;;
	esac
}

# Purpose: Update the destination existence cache entry in the shared runtime
# state.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks after a probe or planning step changes the active context that later
# helpers should use.
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

# Purpose: Return the destination existence cache entry in the form expected by
# later helpers.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when sibling helpers need the same lookup without duplicating module
# logic.
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

# Purpose: Seed the destination existence cache from recursive list so
# incremental work can continue from a valid base.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when zxfer must bootstrap a destination before sending the remaining
# range.
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

# Purpose: Mark the destination root missing in cache in the module-owned
# state.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks so later helpers can make decisions from one shared marker instead of
# re-deriving it.
zxfer_mark_destination_root_missing_in_cache() {
	l_root_dataset=$1

	zxfer_reset_destination_existence_cache
	g_destination_existence_cache_root=$l_root_dataset
	g_destination_existence_cache_root_complete=1
	[ -n "$l_root_dataset" ] && zxfer_set_destination_existence_cache_entry "$l_root_dataset" 0
}

# Purpose: Mark the destination hierarchy exists in the module-owned state.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks so later helpers can make decisions from one shared marker instead of
# re-deriving it.
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

# Purpose: Record the destination dataset exists for later diagnostics or
# control decisions.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when zxfer needs the state preserved for follow-on helpers or
# reporting.
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

# Purpose: Record a successful destination receive in cache state.
# Usage: Called after foreground and supervised receive completion so exact
# receive targets are known-present while descendants are live-probed instead
# of inherited from an old missing-root assumption.
zxfer_note_destination_receive_completed() {
	l_dataset=$1

	[ -n "$l_dataset" ] || return 0
	if [ "${g_destination_existence_cache_root_complete:-0}" -eq 1 ] &&
		[ -n "${g_destination_existence_cache_root:-}" ]; then
		case "$l_dataset" in
		"$g_destination_existence_cache_root" | "$g_destination_existence_cache_root"/*)
			g_destination_existence_cache_root_complete=0
			;;
		esac
	fi

	zxfer_note_destination_dataset_exists "$l_dataset"
}

# Purpose: Extract the snapshot path from the serialized input this module
# works with.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when later helpers need one field or derived fragment without
# reparsing the full payload themselves.
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

# Purpose: Extract the snapshot name from the serialized input this module
# works with.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when later helpers need one field or derived fragment without
# reparsing the full payload themselves.
#
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

# Purpose: Extract the snapshot dataset from the serialized input this module
# works with.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when later helpers need one field or derived fragment without
# reparsing the full payload themselves.
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

# Purpose: Extract the snapshot guid from the serialized input this module
# works with.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when later helpers need one field or derived fragment without
# reparsing the full payload themselves.
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

# Purpose: Extract the snapshot identity from the serialized input this module
# works with.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when later helpers need one field or derived fragment without
# reparsing the full payload themselves.
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

# Purpose: Normalize the snapshot record list into the stable form used across
# zxfer.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks before comparison, caching, or reporting depends on exact formatting.
zxfer_normalize_snapshot_record_list() {
	printf '%s\n' "$1" | tr ' ' '\n'
}

# Purpose: Check whether the snapshot record list contains guid.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when later helpers need a boolean answer about the snapshot record
# list.
zxfer_snapshot_record_list_contains_guid() {
	l_tab='	'
	case "$1" in
	*"$l_tab"*)
		return 0
		;;
	esac

	return 1
}

# Purpose: Reverse the snapshot record list while preserving the record
# structure later helpers rely on.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when comparison or replay logic needs the same data in the opposite
# order.
zxfer_reverse_snapshot_record_list() {
	l_snapshot_records=$1

	[ -n "$l_snapshot_records" ] || return 0

	# shellcheck disable=SC2016  # awk program should see literal $0/NR.
	printf '%s\n' "$l_snapshot_records" | "${g_cmd_awk:-awk}" '{ l_records[NR] = $0 } END { for (l_i = NR; l_i >= 1; l_i--) if (l_records[l_i] != "") print l_records[l_i] }'
}

# Purpose: Transform snapshot records through a staged file and read the result.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when later helpers need a checked reload after normalizing or reversing
# record lists.
zxfer_read_transformed_snapshot_record_list() {
	l_snapshot_records=$1
	l_snapshot_record_transform=$2

	g_zxfer_runtime_artifact_read_result=""
	[ -n "$l_snapshot_records" ] || return 0
	case "$l_snapshot_record_transform" in
	normalized)
		zxfer_capture_runtime_artifact_command_output \
			"zxfer-snapshot-records" \
			zxfer_normalize_snapshot_record_list "$l_snapshot_records"
		;;
	reversed)
		zxfer_capture_runtime_artifact_command_output \
			"zxfer-snapshot-records" \
			zxfer_reverse_snapshot_record_list "$l_snapshot_records"
		;;
	*)
		return 1
		;;
	esac
}

# Purpose: Read the normalized snapshot record list from staged state into the
# current shell.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when later helpers need a checked reload instead of ad hoc file reads.
zxfer_read_normalized_snapshot_record_list() {
	zxfer_read_transformed_snapshot_record_list "$1" normalized
}

# Purpose: Read the reversed snapshot record list from staged state into the
# current shell.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when later helpers need a checked reload instead of ad hoc file reads.
zxfer_read_reversed_snapshot_record_list() {
	zxfer_read_transformed_snapshot_record_list "$1" reversed
}

# Purpose: Check whether the snapshot record lists share snapshot name.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when later helpers need a boolean answer about the snapshot record
# lists.
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

# Purpose: Filter the snapshot identity records to reference paths down to the
# subset later helpers should act on.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks before reconciliation, execution, or reporting consumes the reduced
# set.
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

# Purpose: Return the source snapshot identity records for dataset in the form
# expected by later helpers.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_source_snapshot_identity_records_for_dataset() {
	l_dataset=$1

	if l_snapshot_records=$(zxfer_run_source_zfs_cmd list -H -o name,guid -s creation -d 1 -t snapshot "$l_dataset"); then
		:
	else
		l_status=$?
		return "$l_status"
	fi

	if zxfer_read_normalized_snapshot_record_list "$l_snapshot_records" >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_snapshot_records=$g_zxfer_runtime_artifact_read_result

	if zxfer_read_reversed_snapshot_record_list "$l_snapshot_records" >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi

	printf '%s' "$g_zxfer_runtime_artifact_read_result"
}

# Purpose: Return the destination snapshot identity records for dataset in the
# form expected by later helpers.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_destination_snapshot_identity_records_for_dataset() {
	l_dataset=$1

	if l_snapshot_records=$(zxfer_run_destination_zfs_cmd list -Hr -o name,guid -t snapshot "$l_dataset"); then
		:
	else
		l_status=$?
		return "$l_status"
	fi

	if zxfer_read_normalized_snapshot_record_list "$l_snapshot_records" >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi

	l_filtered_identity_records=""
	while IFS= read -r l_snapshot_record; do
		[ -n "$l_snapshot_record" ] || continue
		l_snapshot_path=$(zxfer_extract_snapshot_path "$l_snapshot_record")
		case "$l_snapshot_path" in
		"$l_dataset"@*)
			if [ -n "$l_filtered_identity_records" ]; then
				l_filtered_identity_records=$l_filtered_identity_records'
'$l_snapshot_record
			else
				l_filtered_identity_records=$l_snapshot_record
			fi
			;;
		esac
	done <<EOF
$g_zxfer_runtime_artifact_read_result
EOF

	printf '%s' "$l_filtered_identity_records"
}

# Purpose: Return the snapshot identity records for dataset in the form
# expected by later helpers.
# Usage: Called during snapshot indexing, cache reads, and destination-state
# checks when sibling helpers need the same lookup without duplicating module
# logic.
zxfer_get_snapshot_identity_records_for_dataset() {
	l_side=$1
	l_dataset=$2
	l_reference_records=${3:-}

	case "$l_side" in
	source)
		if l_identity_records=$(zxfer_get_source_snapshot_identity_records_for_dataset "$l_dataset"); then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		;;
	destination)
		if l_identity_records=$(zxfer_get_destination_snapshot_identity_records_for_dataset "$l_dataset"); then
			:
		else
			l_status=$?
			return "$l_status"
		fi
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
