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
# SNAPSHOT LIST COMMAND PRODUCERS / NORMALIZATION
################################################################################

# Module contract:
# owns globals: source-producer command/PID state, the rendered source
#   listing command (g_zxfer_source_snapshot_list_cmd_result), staged-read and
#   status channels, parallel capability results, origin parallel-command
#   state, the destination listing stderr scratch file, the quoted-path and
#   pipeline status-check render results, ZXFER_SNAPSHOT_RECORD_AWK and
#   ZXFER_SOURCE_DISCOVERY_SENTINEL.
# reads globals: source/destination ZFS commands, remote-origin settings,
#   compression helpers, parallel settings, and snapshot exclude options.
# mutates caches: producer-owned scratch, and the destination existence entry
#   of the destination root after a local listing.
# returns via stdout: bounded staged results and normalized snapshot streams;
#   may launch registered producer processes.

# Awk program over snapshot records (dataset@snapshot<TAB>guid). When
# destination_dataset is set, a record for it or a descendant is rewritten to
# the same path under initial_source. When exclude_pattern is set, records
# whose dataset matches it are dropped.
# shellcheck disable=SC2016  # awk program should see literal $0.
ZXFER_SNAPSHOT_RECORD_AWK='
{
	record = $0
	if (destination_dataset != "" && index(record, destination_dataset) == 1) {
		suffix = substr(record, length(destination_dataset) + 1)
		if (substr(suffix, 1, 1) == "@" || substr(suffix, 1, 1) == "/")
			record = initial_source suffix
	}
	if (exclude_pattern != "") {
		dataset = record
		tab_pos = index(dataset, "\t")
		if (tab_pos > 0)
			dataset = substr(dataset, 1, tab_pos - 1)
		at_pos = index(dataset, "@")
		if (at_pos > 0)
			dataset = substr(dataset, 1, at_pos - 1)
		if (dataset ~ exclude_pattern)
			next
	}
	print record
}'

# The last line of a complete remote or -j source listing.
ZXFER_SOURCE_DISCOVERY_SENTINEL='@@ZXFER_SOURCE_SNAPSHOT_DISCOVERY_COMPLETE@@'

# Purpose: Reset every source/destination producer result for a new discovery.
# Usage: zxfer_reset_snapshot_producer_state; the validated remote-parallel
# cache survives it and is cleared only by the session reset below.
zxfer_reset_snapshot_producer_state() {
	g_source_snapshot_list_cmd=""
	g_zxfer_source_snapshot_list_cmd_result=""
	g_source_snapshot_list_pid=""
	g_source_snapshot_list_uses_parallel=0
	g_source_snapshot_list_uses_metadata_compression=0
	g_source_snapshot_list_sorted_file=""
	g_zxfer_snapshot_discovery_file_read_result=""
	g_zxfer_snapshot_discovery_status_file_result=""
	g_zxfer_parallel_source_job_check_result=""
}

# Purpose: Reset producer state that is valid only within one zxfer session.
# Usage: zxfer_reset_snapshot_producer_session_state; called once by the
# composition root before any discovery pass.
zxfer_reset_snapshot_producer_session_state() {
	zxfer_reset_snapshot_producer_state
	g_origin_parallel_cmd=""
	g_origin_parallel_cmd_host=""
	g_zxfer_destination_listing_error_file=""
}

# Purpose: Read a staged discovery file into the current shell.
# Usage: zxfer_read_snapshot_discovery_capture_file PATH; publishes
# g_zxfer_snapshot_discovery_file_read_result or returns the read status.
zxfer_read_snapshot_discovery_capture_file() {
	l_capture_path=$1

	g_zxfer_snapshot_discovery_file_read_result=""
	zxfer_read_runtime_artifact_file "$l_capture_path" || return "$?"
	g_zxfer_snapshot_discovery_file_read_result=$g_zxfer_runtime_artifact_read_result

	return 0
}

# Purpose: Print the first lines of a captured text for an error message.
# Usage: zxfer_limit_snapshot_discovery_capture_lines TEXT [LIMIT]; LIMIT
# defaults to 10, as does any value that is not a positive number.
zxfer_limit_snapshot_discovery_capture_lines() {
	l_capture_contents=$1
	l_line_limit=${2:-10}
	l_limited_contents=""
	l_line_count=0

	zxfer_is_uint "$l_line_limit" || l_line_limit=10
	[ "$l_line_limit" -gt 0 ] || l_line_limit=10

	while IFS= read -r l_capture_line || [ -n "$l_capture_line" ]; do
		l_line_count=$((l_line_count + 1))
		[ "$l_line_count" -le "$l_line_limit" ] || break
		if [ -n "$l_limited_contents" ]; then
			l_limited_contents=$l_limited_contents'
'$l_capture_line
		else
			l_limited_contents=$l_capture_line
		fi
	done <<EOF
$l_capture_contents
EOF

	printf '%s' "$l_limited_contents"
}

# Purpose: Ensure parallel exists on the host that runs -j source discovery.
# Usage: zxfer_ensure_parallel_available_for_source_jobs; on failure returns
# non-zero with the reason in g_zxfer_parallel_source_job_check_result. A
# throw inside the remote lookup (ssh policy, scratch file) ends the run.
#
# The resolved helper is trusted without a version probe: the rendered
# pipeline uses GNU-compatible options and fails if the helper is not
# compatible. Once -j is requested, discovery never falls back to the serial
# listing.
zxfer_ensure_parallel_available_for_source_jobs() {
	g_zxfer_parallel_source_job_check_result=""

	if [ "$g_option_j_jobs" -le 1 ]; then
		return 0
	fi

	if [ "$g_option_O_origin_host" = "" ]; then
		if [ "$g_cmd_parallel" = "" ]; then
			g_zxfer_parallel_source_job_check_result="The -j option requires parallel but it was not found in PATH on the local host."
			return 1
		fi

		return 0
	fi

	if [ -n "${g_origin_parallel_cmd:-}" ] &&
		[ "${g_origin_parallel_cmd_host:-}" = "$g_option_O_origin_host" ]; then
		return 0
	fi

	zxfer_resolve_remote_required_tool "$g_option_O_origin_host" parallel parallel source || {
		l_remote_parallel_status=$?
		case $g_zxfer_required_tool_result in
		"Required dependency \"parallel\" not found on host "*)
			g_zxfer_parallel_source_job_check_result="parallel not found on origin host $g_option_O_origin_host but -j $g_option_j_jobs was requested. Install parallel remotely or rerun without -j."
			;;
		*)
			g_zxfer_parallel_source_job_check_result=$g_zxfer_required_tool_result
			;;
		esac
		return "$l_remote_parallel_status"
	}
	g_origin_parallel_cmd=$g_zxfer_required_tool_result
	g_origin_parallel_cmd_host=$g_option_O_origin_host
}

# Purpose: Render the local filter that checks and strips the success
# sentinel, the last line of a complete source listing.
# Usage: zxfer_render_discovery_sentinel_filter_cmd; publishes
# g_zxfer_shell_command_result. The filter exits 65 when the sentinel is not
# the last line, so a listing that failed inside parallel or behind the
# origin's compressor, whose status later pipeline stages mask, can never pass
# for a complete one.
zxfer_render_discovery_sentinel_filter_cmd() {
	# shellcheck disable=SC2016  # awk program must see literal $0.
	zxfer_render_shell_command_from_argv "${g_cmd_awk:-awk}" \
		-v sentinel_line="$ZXFER_SOURCE_DISCOVERY_SENTINEL" \
		'NR > 1 { print prev_line } { prev_line = $0 } END { if (NR == 0 || prev_line != sentinel_line) exit 65 }'
}

# Purpose: Render the local command that runs a source listing pipeline on the
# -O host.
# Usage: zxfer_render_origin_listing_command PIPELINE 0|1; the second argument
# says whether PIPELINE ends its output with ZXFER_SOURCE_DISCOVERY_SENTINEL,
# which a local filter then checks and strips. Under -z the origin compresses
# the stream and the local side decompresses it. Publishes
# g_zxfer_source_snapshot_list_cmd_result.
zxfer_render_origin_listing_command() {
	l_origin_listing=$1
	if [ "${g_option_z_compress:-0}" -eq 1 ]; then
		if [ -z "${g_origin_cmd_compress_safe:-}" ]; then
			g_zxfer_source_snapshot_list_cmd_result="The origin host compression command is not resolved."
			return 1
		fi
		g_source_snapshot_list_uses_metadata_compression=1
		l_origin_listing="$l_origin_listing | $g_origin_cmd_compress_safe"
	fi
	zxfer_build_remote_sh_c_command "$l_origin_listing" >/dev/null
	zxfer_ssh_shell_command_for_host render "$g_option_O_origin_host" \
		"$g_zxfer_remote_sh_c_command_result" || return
	l_origin_listing=$g_zxfer_shell_command_result
	[ "$g_source_snapshot_list_uses_metadata_compression" -eq 0 ] ||
		l_origin_listing="$l_origin_listing | $g_cmd_decompress_safe"
	if [ "$2" -eq 1 ]; then
		zxfer_render_discovery_sentinel_filter_cmd
		l_origin_listing="$l_origin_listing | $g_zxfer_shell_command_result"
	fi
	g_zxfer_source_snapshot_list_cmd_result=$l_origin_listing
}

# Purpose: Build the source listing for the recursive no-op proof: one
# recursive name,guid stream of every source snapshot.
# Usage: zxfer_build_source_snapshot_name_list_cmd; publishes the shell
# command in g_zxfer_source_snapshot_list_cmd_result, or returns non-zero with
# an operator message (or nothing) there. The proof ignores -j, so it never
# pays parallel and per-dataset zfs startup before it knows there is work.
# GUIDs keep an exact-name divergence from passing as a no-op.
zxfer_build_source_snapshot_name_list_cmd() {
	g_source_snapshot_list_uses_parallel=0
	g_source_snapshot_list_uses_metadata_compression=0
	g_zxfer_source_snapshot_list_cmd_result=""

	if [ -z "$g_option_O_origin_host" ]; then
		zxfer_render_shell_command_from_argv "$g_cmd_zfs" \
			list -Hr -o name,guid -t snapshot "$g_initial_source"
		g_zxfer_source_snapshot_list_cmd_result=$g_zxfer_shell_command_result
		return 0
	fi

	zxfer_render_shell_command_from_argv "${g_origin_cmd_zfs:-$g_cmd_zfs}" \
		list -Hr -o name,guid -t snapshot "$g_initial_source"
	l_name_list_pipeline=$g_zxfer_shell_command_result
	l_name_list_sentinel=0
	if [ "${g_option_z_compress:-0}" -eq 1 ]; then
		# The origin's compressor masks the listing's status (the remote sh has
		# no pipefail), so a listing that died mid-stream would look complete
		# and could let the proof conclude nothing needs to transfer. Only a
		# successful listing adds the sentinel.
		l_name_list_pipeline="{ $l_name_list_pipeline && printf '%s\n' '$ZXFER_SOURCE_DISCOVERY_SENTINEL'; }"
		l_name_list_sentinel=1
	fi
	zxfer_render_origin_listing_command "$l_name_list_pipeline" "$l_name_list_sentinel"
}

# Purpose: Quote one path as a single shell word, without forking.
# Usage: zxfer_quote_path_into_result PATH; sets g_zxfer_quoted_path_result
# to the text zxfer_render_shell_command_from_argv PATH publishes.
zxfer_quote_path_into_result() {
	zxfer_escape_single_quotes_into_result "$1"
	g_zxfer_quoted_path_result="'$g_zxfer_escaped_single_quotes_result'"
}

# Purpose: Render the text that ends a background sort pipeline whose stages
# record their exit statuses in files.
# Usage: zxfer_render_pipeline_status_check "QUOTED_STATUS_FILE..."; sets
# g_zxfer_pipeline_status_check_result. The text removes the status files and
# exits with the first stage status that is non-zero or unreadable, otherwise
# with sort's status, so a failed stage cannot pass for a short listing.
zxfer_render_pipeline_status_check() {
	g_zxfer_pipeline_status_check_result="l_sort_status=\$?; l_failed_status=0; for l_status_file in $1; do l_status=1; if [ -f \"\$l_status_file\" ]; then IFS= read -r l_status < \"\$l_status_file\" || l_status=1; fi; case \$l_status in '' | *[!0-9]*) l_status=1 ;; esac; [ \"\$l_failed_status\" -ne 0 ] || l_failed_status=\$l_status; done; rm -f $1; [ \"\$l_failed_status\" -eq 0 ] || exit \"\$l_failed_status\"; exit \"\$l_sort_status\""
}

# Purpose: Register a just-spawned background producer for trap cleanup, or
# stop and reap it when registration fails.
# Usage: zxfer_register_background_producer PURPOSE; acts on
# g_last_background_pid and g_zxfer_background_shell_scope, and returns 1
# once an unregistered producer is stopped (or could not be).
zxfer_register_background_producer() {
	zxfer_register_cleanup_pid "$g_last_background_pid" "$1" \
		"$g_zxfer_background_shell_scope" && return 0
	zxfer_abort_direct_child_pid "$g_last_background_pid" TERM "$1" \
		"$g_zxfer_background_shell_scope" || return 1
	# A group leader can exit on TERM while descendants keep running.
	zxfer_cleanup_pid_abort_grace_wait
	zxfer_abort_direct_child_pid "$g_last_background_pid" KILL "$1" \
		"$g_zxfer_background_shell_scope" || return 1
	wait "$g_last_background_pid" 2>/dev/null || :
	zxfer_unregister_cleanup_pid "$g_last_background_pid"
	g_last_background_pid=""
	return 1
}

# Purpose: Run the fast no-op proof's source command in the background and
# byte-sort its records into a file.
# Usage: zxfer_execute_source_snapshot_name_list_background_sort_cmd CMD
# SORTED_FILE [ERROR_FILE] [COUNT_FILE]; with -x the records pass the exclude
# filter first. COUNT_FILE receives 1 when at least one record reached sort,
# otherwise 0.
# Side effects: Publishes the registered producer PID in g_last_background_pid.
zxfer_execute_source_snapshot_name_list_background_sort_cmd() {
	l_cmd=$1
	l_sorted_output_file=$2
	l_error_file=${3:-}
	l_count_file=${4:-}

	zxfer_get_temp_file || return "$?"
	zxfer_quote_path_into_result "$g_zxfer_temp_file_result"
	l_status_files=$g_zxfer_quoted_path_result
	l_managed_cmd="{ ( $l_cmd ); printf '%s\n' \"\$?\" > $g_zxfer_quoted_path_result; }"
	if [ -n "${g_option_x_exclude_datasets:-}" ]; then
		zxfer_get_temp_file || return "$?"
		zxfer_quote_path_into_result "$g_zxfer_temp_file_result"
		l_status_files="$l_status_files $g_zxfer_quoted_path_result"
		zxfer_render_shell_command_from_argv "${g_cmd_awk:-awk}" \
			-v "exclude_pattern=$g_option_x_exclude_datasets" "$ZXFER_SNAPSHOT_RECORD_AWK"
		l_managed_cmd="$l_managed_cmd | { $g_zxfer_shell_command_result; printf '%s\n' \"\$?\" > $g_zxfer_quoted_path_result; }"
	fi
	if [ -n "$l_count_file" ]; then
		zxfer_get_temp_file || return "$?"
		zxfer_quote_path_into_result "$g_zxfer_temp_file_result"
		l_count_status_file_q=$g_zxfer_quoted_path_result
		l_status_files="$l_status_files $l_count_status_file_q"
		zxfer_quote_path_into_result "$l_count_file"
		l_managed_cmd="$l_managed_cmd | { l_count_status=0; if IFS= read -r l_first_snapshot; then printf '%s\n' 1 > $g_zxfer_quoted_path_result || l_count_status=\$?; printf '%s\n' \"\$l_first_snapshot\" || l_count_status=\$?; cat || l_count_status=\$?; else printf '%s\n' 0 > $g_zxfer_quoted_path_result || l_count_status=\$?; fi; printf '%s\n' \"\$l_count_status\" > $l_count_status_file_q; exit \"\$l_count_status\"; }"
	fi
	zxfer_render_pipeline_status_check "$l_status_files"
	zxfer_quote_path_into_result "$l_sorted_output_file"
	l_managed_cmd="$l_managed_cmd | LC_ALL=C sort > $g_zxfer_quoted_path_result; $g_zxfer_pipeline_status_check_result"

	zxfer_echoV "Executing command in the background: $l_managed_cmd"
	zxfer_record_last_command_string "$l_managed_cmd"
	zxfer_spawn_background_shell "$l_managed_cmd" /dev/null "$l_error_file" || return "$?"
	zxfer_register_background_producer "background source snapshot no-op proof helper"
}

# Purpose: Build the creation-ordered source snapshot listing.
# Usage: zxfer_build_source_snapshot_list_cmd; publishes the shell command in
# g_zxfer_source_snapshot_list_cmd_result, or returns non-zero with an
# operator message (or nothing) there. With -j the listing fans out over the
# source datasets through parallel: the dataset list is captured first (exit
# 70 on failure), and only a successful parallel run adds the success sentinel
# that a local filter checks and strips.
zxfer_build_source_snapshot_list_cmd() {
	g_source_snapshot_list_uses_parallel=0
	g_source_snapshot_list_uses_metadata_compression=0
	g_zxfer_source_snapshot_list_cmd_result=""

	if [ "$g_option_j_jobs" -le 1 ]; then
		zxfer_render_zfs_command_for_role source \
			list -Hr -o name,guid -s creation -t snapshot "$g_initial_source" || return
		g_zxfer_source_snapshot_list_cmd_result=$g_zxfer_shell_command_result
		return 0
	fi

	zxfer_ensure_parallel_available_for_source_jobs || {
		l_list_status=$?
		g_zxfer_source_snapshot_list_cmd_result=${g_zxfer_parallel_source_job_check_result:-Failed to prepare parallel source discovery.}
		return "$l_list_status"
	}
	g_source_snapshot_list_uses_parallel=1
	if [ -n "$g_option_O_origin_host" ]; then
		l_list_zfs=${g_origin_cmd_zfs:-$g_cmd_zfs}
		l_list_parallel=$g_origin_parallel_cmd
	else
		l_list_zfs=$g_cmd_zfs
		l_list_parallel=$g_cmd_parallel
	fi
	zxfer_render_shell_command_from_argv "$l_list_zfs" \
		list -Hr -t filesystem,volume -o name "$g_initial_source"
	l_list_datasets=$g_zxfer_shell_command_result
	# The placeholder stays bare: GNU parallel shell-quotes each dataset it
	# substitutes for {}, and a quoted '{}' would cancel that quoting and
	# split names that contain spaces.
	zxfer_render_shell_command_from_argv "$l_list_zfs" \
		list -H -o name,guid -s creation -d 1 -t snapshot
	l_list_runner="$g_zxfer_shell_command_result {}"
	zxfer_render_shell_command_from_argv "$l_list_parallel"
	# Piped straight into parallel, a failed dataset listing would look like
	# an empty one and the sentinel would mark the listing complete. Behind
	# ssh the exit codes are masked (no pipefail), so there the failure shows
	# as the missing sentinel plus the remote stderr.
	l_list_pipeline="zxfer_discovery_datasets=\$($l_list_datasets) || exit 70; { printf '%s\n' \"\$zxfer_discovery_datasets\" | $g_zxfer_shell_command_result -j $g_option_j_jobs --line-buffer -- \"$l_list_runner\" && printf '%s\n' '$ZXFER_SOURCE_DISCOVERY_SENTINEL'; }"
	if [ -n "$g_option_O_origin_host" ]; then
		zxfer_render_origin_listing_command "$l_list_pipeline" 1
		return
	fi
	zxfer_render_discovery_sentinel_filter_cmd
	g_zxfer_source_snapshot_list_cmd_result="$l_list_pipeline | $g_zxfer_shell_command_result"
}

# Purpose: Run the full source listing command in the background, keeping its
# creation-ordered output and a byte-sorted copy.
# Usage: zxfer_execute_source_snapshot_list_background_cmd_with_sort CMD
# OUTPUT_FILE ERROR_FILE SORTED_FILE; an empty SORTED_FILE runs CMD alone.
# Side effects: Publishes the registered producer PID in g_last_background_pid.
zxfer_execute_source_snapshot_list_background_cmd_with_sort() {
	l_source_background_command=$1
	l_source_background_output_file=$2
	l_source_background_error_file=${3:-}
	l_sorted_output_file=$4

	if [ -z "$l_sorted_output_file" ]; then
		zxfer_execute_rendered_background_shell_command \
			"$l_source_background_command" \
			"$l_source_background_output_file" \
			"$l_source_background_error_file"
		return "$?"
	fi

	zxfer_create_temp_file_group 2 || return "$?"
	{
		IFS= read -r l_source_status_file
		IFS= read -r l_tee_status_file
	} <<-EOF
		$g_zxfer_temp_file_group_result
	EOF
	zxfer_quote_path_into_result "$l_source_status_file"
	l_source_status_file_q=$g_zxfer_quoted_path_result
	zxfer_quote_path_into_result "$l_tee_status_file"
	l_tee_status_file_q=$g_zxfer_quoted_path_result
	zxfer_render_pipeline_status_check "$l_source_status_file_q $l_tee_status_file_q"
	zxfer_quote_path_into_result "$l_source_background_output_file"
	l_managed_cmd="{ ( $l_source_background_command ); printf '%s\n' \"\$?\" > $l_source_status_file_q; } | { tee $g_zxfer_quoted_path_result; printf '%s\n' \"\$?\" > $l_tee_status_file_q; }"
	zxfer_quote_path_into_result "$l_sorted_output_file"
	l_managed_cmd="$l_managed_cmd | LC_ALL=C sort > $g_zxfer_quoted_path_result; $g_zxfer_pipeline_status_check_result"

	zxfer_echoV "Executing command in the background: $l_managed_cmd"
	zxfer_record_last_command_string "$l_managed_cmd"
	zxfer_spawn_background_shell "$l_managed_cmd" /dev/null \
		"$l_source_background_error_file" || return "$?"
	zxfer_register_background_producer "background source snapshot discovery helper"
}

# Purpose: Start the creation-ordered source snapshot listing in the background.
# Usage: zxfer_write_source_snapshot_list_to_file OUTFILE [ERRFILE]; publishes
# g_source_snapshot_list_pid, and the byte-sorted copy in
# g_source_snapshot_list_sorted_file when
# g_source_snapshot_list_background_sort_requested is 1. With -j the listing
# fans out over datasets through parallel.
zxfer_write_source_snapshot_list_to_file() {
	l_outfile=$1
	l_errfile=${2:-}
	l_sorted_outfile=""
	zxfer_profile_increment_counter g_zxfer_profile_source_snapshot_list_commands
	zxfer_profile_increment_counter g_zxfer_profile_bucket_source_inspection

	#
	# it is important to get this in ascending order because when getting
	# in descending order, the datasets names are not ordered as we want.
	# Don't use -S creation for this command, instead, reverse the results below
	#
	zxfer_build_source_snapshot_list_cmd ||
		zxfer_throw_error "${g_zxfer_source_snapshot_list_cmd_result:-Failed to build source snapshot discovery command.}" "$?"
	l_source_snapshot_command=$g_zxfer_source_snapshot_list_cmd_result
	g_source_snapshot_list_cmd=$l_source_snapshot_command
	if [ "$g_option_O_origin_host" != "" ]; then
		zxfer_profile_record_ssh_invocation "$g_option_O_origin_host" source
	fi

	if [ "${g_source_snapshot_list_uses_parallel:-0}" -eq 1 ]; then
		zxfer_profile_increment_counter g_zxfer_profile_source_snapshot_list_parallel_commands
	fi
	zxfer_echoV "Running command in the background: $l_source_snapshot_command"
	zxfer_record_last_command_string "$l_source_snapshot_command"
	if [ "${g_source_snapshot_list_background_sort_requested:-0}" -eq 1 ]; then
		zxfer_get_temp_file || return "$?"
		l_sorted_outfile=$g_zxfer_temp_file_result
		g_source_snapshot_list_sorted_file=$l_sorted_outfile
		zxfer_execute_source_snapshot_list_background_cmd_with_sort \
			"$l_source_snapshot_command" "$l_outfile" \
			"$l_errfile" "$l_sorted_outfile" || {
			l_source_list_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_sorted_outfile"
			g_source_snapshot_list_sorted_file=""
			return "$l_source_list_status"
		}
	else
		zxfer_execute_rendered_background_shell_command \
			"$l_source_snapshot_command" "$l_outfile" "$l_errfile" ||
			return "$?"
	fi
	g_source_snapshot_list_pid=$g_last_background_pid
}

# Purpose: Rewrite a destination snapshot listing to source paths and sort it.
# Usage: zxfer_normalize_destination_snapshot_list DEST_DATASET INPUT OUTPUT;
# OUTPUT gets the byte-sorted records that comm compares with the source
# listing. The -x filter is not applied here.
zxfer_normalize_destination_snapshot_list() {
	if zxfer_command_trace_enabled; then
		zxfer_trace_rendered_command "Running command" \
			"$(zxfer_render_command_for_report "" "${g_cmd_awk:-awk}" \
				-v "destination_dataset=$1" -v "initial_source=$g_initial_source" \
				"$ZXFER_SNAPSHOT_RECORD_AWK" "$2") > $(zxfer_quote_token_for_report "$3") && $(zxfer_render_command_for_report "LC_ALL=C" sort -o "$3" "$3")"
	else
		zxfer_record_last_command_opaque
	fi
	"${g_cmd_awk:-awk}" -v "destination_dataset=$1" -v "initial_source=$g_initial_source" \
		"$ZXFER_SNAPSHOT_RECORD_AWK" "$2" >"$3" || return "$?"
	# POSIX lets sort -o name one of its own input files.
	LC_ALL=C sort -o "$3" "$3"
}

# Purpose: Rewrite destination snapshot records on stdin to source paths and
# drop the -x matches, for the fast no-op proof.
# Usage: zxfer_normalize_destination_snapshot_stream_for_noop_proof DEST_DATASET
zxfer_normalize_destination_snapshot_stream_for_noop_proof() {
	"${g_cmd_awk:-awk}" -v "destination_dataset=$1" -v "initial_source=$g_initial_source" \
		-v "exclude_pattern=${g_option_x_exclude_datasets:-}" "$ZXFER_SNAPSHOT_RECORD_AWK"
}

# Purpose: Read the numeric status a background discovery stage wrote.
# Usage: zxfer_read_snapshot_discovery_status_file FILE [DEFAULT]; publishes
# the status (DEFAULT, 1 by default, when FILE is missing or unreadable) in
# g_zxfer_snapshot_discovery_status_file_result, and returns 1 when it is not
# a number.
zxfer_read_snapshot_discovery_status_file() {
	g_zxfer_snapshot_discovery_status_file_result=${2:-1}
	if [ -f "$1" ]; then
		IFS= read -r g_zxfer_snapshot_discovery_status_file_result <"$1" ||
			g_zxfer_snapshot_discovery_status_file_result=${2:-1}
	fi
	zxfer_is_uint "$g_zxfer_snapshot_discovery_status_file_result"
}

# Purpose: Publish a staged destination dataset inventory as g_recursive_dest_list
# and seed the existence cache from it.
# Usage: zxfer_publish_destination_dataset_inventory_from_stage LIST_FILE
# ERR_FILE STATUS [POOL_STATUS]; shared by local and remote discovery. A
# missing destination whose pool exists publishes an empty list; other
# failures throw.
zxfer_publish_destination_dataset_inventory_from_stage() {
	l_destination_inventory_tmp_file=$1
	l_destination_inventory_err_file=$2
	l_destination_inventory_status=$3
	l_destination_inventory_pool_status=${4:-}

	if [ "$l_destination_inventory_status" -eq 0 ]; then
		zxfer_read_snapshot_discovery_capture_file \
			"$l_destination_inventory_tmp_file" ||
			zxfer_throw_error "Failed to read staged destination dataset inventory." "$?"
		g_recursive_dest_list=$g_zxfer_snapshot_discovery_file_read_result
		[ -n "$g_recursive_dest_list" ] || {
			zxfer_throw_error "Staged destination dataset inventory was empty."
		}
		zxfer_seed_destination_existence_cache_from_recursive_list "$g_destination" "$g_recursive_dest_list"
		return
	fi

	zxfer_read_snapshot_discovery_capture_file \
		"$l_destination_inventory_err_file" ||
		zxfer_throw_error "Failed to read staged destination dataset inventory stderr." "$?"
	l_destination_inventory_error=$g_zxfer_snapshot_discovery_file_read_result
	if zxfer_destination_probe_reports_missing \
		"$l_destination_inventory_error"; then
		if [ -z "$l_destination_inventory_pool_status" ]; then
			l_destination_inventory_pool=${g_destination%%/*}
			l_destination_inventory_pool_status=0
			l_destination_inventory_pool_error=$(zxfer_run_destination_zfs_cmd \
				list -H -o name "$l_destination_inventory_pool" 2>&1 >/dev/null) ||
				l_destination_inventory_pool_status=$?
		else
			l_destination_inventory_pool=${g_destination%%/*}
			l_destination_inventory_pool_error=""
		fi
		if [ "$l_destination_inventory_pool_status" -eq 0 ]; then
			g_recursive_dest_list=""
			zxfer_mark_destination_root_missing_in_cache "$g_destination"
			zxfer_echoV "Destination dataset missing; treating as empty list for bootstrap."
		else
			l_destination_inventory_pool_error=$(zxfer_limit_snapshot_discovery_capture_lines \
				"$l_destination_inventory_pool_error" 5)
			if [ -n "$l_destination_inventory_pool_error" ]; then
				zxfer_throw_error "Destination dataset [$g_destination] is missing and destination pool [$l_destination_inventory_pool] could not be listed: $l_destination_inventory_pool_error" "$l_destination_inventory_pool_status"
			fi
			zxfer_throw_error "Destination dataset [$g_destination] is missing and destination pool [$l_destination_inventory_pool] could not be listed." "$l_destination_inventory_pool_status"
		fi
	else
		l_destination_inventory_error=$(zxfer_limit_snapshot_discovery_capture_lines \
			"$l_destination_inventory_error" 5)
		if [ -n "$l_destination_inventory_error" ]; then
			zxfer_throw_error "Failed to retrieve list of datasets from the destination: $l_destination_inventory_error" "$l_destination_inventory_status"
		fi
		zxfer_throw_error "Failed to retrieve list of datasets from the destination" "$l_destination_inventory_status"
	fi
}

# Purpose: Stage the destination root's snapshot listing and its normalized,
# sorted form.
# Usage: zxfer_write_destination_snapshot_list_to_files RAW SORTED; a missing
# destination root leaves both files empty. The listing is serial and has no
# creation order, since the destination side needs neither.
zxfer_write_destination_snapshot_list_to_files() {
	l_dest_list_raw_file=$1
	l_dest_list_sorted_file=$2

	zxfer_map_destination_dataset
	l_dest_list_dataset=$g_zxfer_destination_dataset_result
	zxfer_ensure_snapshot_scratch_file "${g_zxfer_destination_listing_error_file:-}" \
		zxfer-destination-listing-err ||
		zxfer_throw_error "Failed to allocate the destination snapshot listing error file." "$?"
	g_zxfer_destination_listing_error_file=$g_zxfer_snapshot_scratch_file_result
	if zxfer_command_trace_enabled; then
		zxfer_trace_rendered_command "Running command" \
			"$(zxfer_render_destination_zfs_command list -Hr -o name,guid -t snapshot "$l_dest_list_dataset")"
	else
		zxfer_record_last_command_opaque
	fi
	if zxfer_run_destination_zfs_cmd list -Hr -o name,guid -t snapshot "$l_dest_list_dataset" \
		>"$l_dest_list_raw_file" 2>|"$g_zxfer_destination_listing_error_file"; then
		zxfer_set_destination_existence_cache_entry "$l_dest_list_dataset" 1
		# Pass on any warnings of a successful listing.
		if [ -s "$g_zxfer_destination_listing_error_file" ]; then
			cat "$g_zxfer_destination_listing_error_file" >&2 || :
		fi
	else
		l_dest_list_status=$?
		# Its stderr does not reliably say which dataset is missing, so an
		# exact probe (with the SunOS fallback) decides whether the root is
		# absent, the bootstrap case, or the listing itself failed.
		zxfer_probe_destination_existence "$l_dest_list_dataset" live ||
			zxfer_throw_error "$g_zxfer_destination_exists_error" "$?"
		if [ "$g_zxfer_destination_exists_result" -ne 0 ]; then
			if zxfer_read_snapshot_discovery_capture_file "$g_zxfer_destination_listing_error_file" &&
				[ -n "$g_zxfer_snapshot_discovery_file_read_result" ]; then
				zxfer_warn_stderr "$g_zxfer_snapshot_discovery_file_read_result"
			fi
			zxfer_throw_error "Failed to retrieve snapshot list from the destination." "$l_dest_list_status"
		fi
		zxfer_echoV "Destination dataset does not exist: $l_dest_list_dataset"
		zxfer_write_runtime_artifact_file "$l_dest_list_raw_file" "" ||
			zxfer_throw_error "Failed to stage empty destination snapshot list." "$?"
	fi

	zxfer_normalize_destination_snapshot_list "$l_dest_list_dataset" \
		"$l_dest_list_raw_file" "$l_dest_list_sorted_file"
}

# Purpose: Start the fast no-op proof's destination producer in the background.
# Usage: zxfer_start_destination_snapshot_name_sorted_fifo_producer OUTPUT
# ERR_FILE STATUS_FILE RAW_FILE; the producer lists the destination root into
# RAW_FILE (kept for full discovery if the proof declines), writes its
# normalized, -x filtered, byte-sorted form to OUTPUT, and finally writes the
# listing, normalize and sort statuses to STATUS_FILE as one
# "LIST NORMALIZE SORT" line.
# Side effects: Publishes the registered producer PID in g_last_background_pid.
zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
	l_destination_fifo=$1
	l_dest_snapshot_err_file=$2
	l_dest_snapshot_status_file=$3
	l_dest_snapshot_raw_file=$4

	zxfer_map_destination_dataset
	l_destination_dataset=$g_zxfer_destination_dataset_result
	if zxfer_command_trace_enabled; then
		zxfer_trace_rendered_command "Running command in the background" \
			"$(zxfer_render_destination_zfs_command list -Hr -o name,guid -t snapshot "$l_destination_dataset") > $(zxfer_quote_token_for_report "$l_dest_snapshot_raw_file"); $(zxfer_render_command_for_report "" zxfer_normalize_destination_snapshot_stream_for_noop_proof "$l_destination_dataset") < $(zxfer_quote_token_for_report "$l_dest_snapshot_raw_file") > $(zxfer_quote_token_for_report "$l_destination_fifo"); $(zxfer_render_command_for_report "LC_ALL=C" sort -o "$l_destination_fifo" "$l_destination_fifo")"
	else
		zxfer_record_last_command_opaque
	fi

	(
		zxfer_run_destination_zfs_cmd list -Hr -o name,guid -t snapshot "$l_destination_dataset" \
			>"$l_dest_snapshot_raw_file" 2>"$l_dest_snapshot_err_file"
		l_list_status=$?
		zxfer_normalize_destination_snapshot_stream_for_noop_proof "$l_destination_dataset" \
			<"$l_dest_snapshot_raw_file" >"$l_destination_fifo"
		l_normalize_status=$?
		LC_ALL=C sort -o "$l_destination_fifo" "$l_destination_fifo"
		l_sort_status=$?
		printf '%s %s %s\n' "$l_list_status" "$l_normalize_status" "$l_sort_status" \
			>"$l_dest_snapshot_status_file" 2>/dev/null
	) &
	g_last_background_pid=$!
	if ! zxfer_register_cleanup_pid \
		"$g_last_background_pid" "background destination snapshot no-op proof helper"; then
		zxfer_abort_fast_noop_background_pid \
			"$g_last_background_pid" \
			"background destination snapshot no-op proof helper" || return "$?"
		wait "$g_last_background_pid" 2>/dev/null || :
		zxfer_unregister_cleanup_pid "$g_last_background_pid"
		g_last_background_pid=""
		return 1
	fi

	return 0
}

# Purpose: Stop a background no-op proof producer that may be blocked on a
# FIFO open or write.
# Usage: Called on setup, mismatch, and compare-failure paths before waiting on
# the producer so broken compare setup cannot leave source or destination
# helpers stuck behind unopened FIFOs.
zxfer_abort_fast_noop_background_pid() {
	l_fast_noop_abort_pid=$1
	l_fast_noop_abort_purpose=$2

	zxfer_is_uint "$l_fast_noop_abort_pid" || return 0
	if zxfer_find_cleanup_pid_record "$l_fast_noop_abort_pid"; then
		zxfer_abort_cleanup_pid "$l_fast_noop_abort_pid" TERM
		return "$?"
	fi
	zxfer_abort_direct_child_pid \
		"$l_fast_noop_abort_pid" TERM "$l_fast_noop_abort_purpose"
}
