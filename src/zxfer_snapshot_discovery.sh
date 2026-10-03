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
# SNAPSHOT DISCOVERY: LISTINGS / NO-OP PROOF / FULL DISCOVERY / DELTAS
################################################################################

# Module contract:
# owns globals: the source listing's command, PID, flags and remote parallel
#   helper (g_source_snapshot_list_*, g_origin_parallel_cmd, _host), the fast
#   no-op proof's destination listing handed to full discovery, the destination
#   listing stderr scratch file, the staged snapshot record files
#   (g_zxfer_source_snapshot_record_cache_file,
#   g_zxfer_destination_snapshot_record_cache_file), the recursive work lists
#   (g_recursive_source_list, g_recursive_source_dataset_list,
#   g_recursive_destination_extra_dataset_list), and the in-shell results
#   g_zxfer_source_snapshot_list_cmd_result,
#   g_zxfer_parallel_source_job_check_result,
#   g_zxfer_snapshot_discovery_file_read_result,
#   g_zxfer_snapshot_discovery_status_file_result,
#   g_zxfer_snapshot_discovery_failure_result,
#   g_zxfer_recursive_dataset_list_result, g_zxfer_quoted_path_result and
#   g_zxfer_pipeline_status_check_result; also ZXFER_SNAPSHOT_RECORD_AWK and
#   ZXFER_SOURCE_DISCOVERY_SENTINEL.
# reads globals: discovery options (-R, -j, -x, -z, -O, -T, -P, -o, -U, -d,
#   -v, -V), g_initial_source, g_destination, the source and destination zfs,
#   awk, parallel and compression commands, and profiling state.
# mutates caches: the destination existence cache and dataset inventory,
#   through the destination-state helpers.
# returns via stdout: normalized snapshot streams, reversed record files and
#   the -v/-V delta report; launches registered background producers.
#
# zxfer_get_zfs_list first tries the fast recursive no-op proof, then runs
# full discovery: the creation-ordered source listing in the background while
# the destination is listed, the diff that publishes the work lists, and the
# record files per-dataset planning reads. Every destination listing runs
# through zxfer_run_destination_zfs_cmd, so a -T target is listed like a local
# destination, over its ssh control master.
#
# Discovery stages at most one flat snapshot record file per side
# ("dataset@snapshot<TAB>guid" rows) inside the 0700 run-private temp root:
# g_zxfer_source_snapshot_record_cache_file (newest first) and
# g_zxfer_destination_snapshot_record_cache_file. Those files are the
# snapshot-record index that per-dataset planning reads. They are never
# legitimately mutated afterwards, so a staged file that cannot be read is
# corrupted run-private state and aborts the run.

################################################################################
# SOURCE AND DESTINATION LISTING COMMANDS AND PRODUCERS
################################################################################

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
	l_origin_listing_compressed=0
	if [ "${g_option_z_compress:-0}" -eq 1 ]; then
		if [ -z "${g_origin_cmd_compress_safe:-}" ]; then
			g_zxfer_source_snapshot_list_cmd_result="The origin host compression command is not resolved."
			return 1
		fi
		l_origin_listing_compressed=1
		l_origin_listing="$l_origin_listing | $g_origin_cmd_compress_safe"
	fi
	zxfer_build_remote_sh_c_command "$l_origin_listing" >/dev/null
	zxfer_ssh_shell_command_for_host render "$g_option_O_origin_host" \
		"$g_zxfer_remote_sh_c_command_result" || return
	l_origin_listing=$g_zxfer_shell_command_result
	[ "$l_origin_listing_compressed" -eq 0 ] ||
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
# OUTPUT_FILE ERROR_FILE SORTED_FILE
# Side effects: Publishes the registered producer PID in g_last_background_pid.
zxfer_execute_source_snapshot_list_background_cmd_with_sort() {
	l_source_background_command=$1
	l_source_background_output_file=$2
	l_source_background_error_file=${3:-}
	l_sorted_output_file=$4

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
# Usage: zxfer_write_source_snapshot_list_to_file OUTFILE ERRFILE SORTED_FILE;
# the operation owner allocates all three paths. Publishes the registered
# g_source_snapshot_list_pid. With -j the listing fans out through parallel.
zxfer_write_source_snapshot_list_to_file() {
	l_outfile=$1
	l_errfile=$2
	g_zxfer_profile_source_snapshot_list_commands=$((g_zxfer_profile_source_snapshot_list_commands + 1))
	g_zxfer_profile_bucket_source_inspection=$((g_zxfer_profile_bucket_source_inspection + 1))

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
		g_zxfer_profile_source_ssh_shell_invocations=$((g_zxfer_profile_source_ssh_shell_invocations + 1))
	fi

	if [ "${g_source_snapshot_list_uses_parallel:-0}" -eq 1 ]; then
		g_zxfer_profile_source_snapshot_list_parallel_commands=$((g_zxfer_profile_source_snapshot_list_parallel_commands + 1))
	fi
	zxfer_echoV "Running command in the background: $l_source_snapshot_command"
	zxfer_record_last_command_string "$l_source_snapshot_command"
	zxfer_execute_source_snapshot_list_background_cmd_with_sort \
		"$l_source_snapshot_command" "$l_outfile" "$l_errfile" "$3" || return "$?"
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

# Purpose: Publish a staged destination dataset inventory through the
# destination-state seed.
# Usage: zxfer_publish_destination_dataset_inventory_from_stage LIST_FILE
# ERR_FILE STATUS; STATUS is the listing's exit status. A missing destination
# whose pool the live probe can list publishes an empty inventory; other
# failures throw.
zxfer_publish_destination_dataset_inventory_from_stage() {
	l_destination_inventory_tmp_file=$1
	l_destination_inventory_err_file=$2
	l_destination_inventory_status=$3

	if [ "$l_destination_inventory_status" -eq 0 ]; then
		zxfer_read_snapshot_discovery_capture_file \
			"$l_destination_inventory_tmp_file" ||
			zxfer_throw_error "Failed to read staged destination dataset inventory." "$?"
		[ -n "$g_zxfer_snapshot_discovery_file_read_result" ] || {
			zxfer_throw_error "Staged destination dataset inventory was empty."
		}
		zxfer_seed_destination_existence_cache_from_recursive_list "$g_destination" \
			"$g_zxfer_snapshot_discovery_file_read_result"
		return
	fi

	zxfer_read_snapshot_discovery_capture_file \
		"$l_destination_inventory_err_file" ||
		zxfer_throw_error "Failed to read staged destination dataset inventory stderr." "$?"
	l_destination_inventory_error=$g_zxfer_snapshot_discovery_file_read_result
	if zxfer_destination_probe_reports_missing \
		"$l_destination_inventory_error"; then
		# The root is missing; the run can bootstrap it only when its pool
		# is really there.
		l_destination_inventory_pool=${g_destination%%/*}
		l_destination_inventory_pool_status=0
		l_destination_inventory_pool_error=$(zxfer_run_destination_zfs_cmd \
			list -H -o name "$l_destination_inventory_pool" 2>&1 >/dev/null) ||
			l_destination_inventory_pool_status=$?
		if [ "$l_destination_inventory_pool_status" -eq 0 ]; then
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
# creation order, since the destination side needs neither. A -T listing that
# ssh could not deliver (status 255) stops the run with that status.
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
		# ssh exits 255 for its own failures and zfs never does, so a -T
		# listing that ssh could not deliver says nothing about the dataset:
		# the run stops with ssh's status instead of probing over the same
		# connection. Otherwise the listing's stderr does not reliably say
		# which dataset is missing, so an exact probe (with the SunOS
		# fallback) decides whether the root is absent, the bootstrap case,
		# or the listing itself failed.
		l_dest_list_exists=1
		if [ "$l_dest_list_status" -ne 255 ] || [ -z "${g_option_T_target_host:-}" ]; then
			zxfer_probe_destination_existence "$l_dest_list_dataset" live ||
				zxfer_throw_error "$g_zxfer_destination_exists_error" "$?"
			l_dest_list_exists=$g_zxfer_destination_exists_result
		fi
		if [ "$l_dest_list_exists" -ne 0 ]; then
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

################################################################################
# DISCOVERY STATE, DELTAS AND THE TWO DISCOVERY PATHS
################################################################################

# Purpose: Remove and forget the published snapshot record files.
# Usage: zxfer_cleanup_snapshot_record_cache_files, before a discovery pass and
# on its failure paths.
zxfer_cleanup_snapshot_record_cache_files() {
	if [ -n "${g_zxfer_source_snapshot_record_cache_file:-}" ]; then
		zxfer_cleanup_runtime_artifact_path "$g_zxfer_source_snapshot_record_cache_file"
	fi
	if [ -n "${g_zxfer_destination_snapshot_record_cache_file:-}" ]; then
		zxfer_cleanup_runtime_artifact_path "$g_zxfer_destination_snapshot_record_cache_file"
	fi

	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
}

# Purpose: Reset the snapshot discovery state so the next pass starts clean.
# Usage: zxfer_reset_snapshot_discovery_state; called at the start of each
# discovery pass and by session and dry-run resets. The destination existence
# cache and inventory have their own reset in the destination-state module.
zxfer_reset_snapshot_discovery_state() {
	zxfer_cleanup_snapshot_record_cache_files
	zxfer_reset_snapshot_producer_state
	g_zxfer_snapshot_discovery_failure_result=""
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_recursive_dataset_list_result=""
	g_zxfer_snapshot_discovery_destination_listing_file=""
}

# Purpose: Publish the -n preview's work lists: the initial source alone.
# Usage: zxfer_publish_dry_run_snapshot_discovery, instead of
# zxfer_get_zfs_list under -n. It runs no zfs command and first drops the
# discovery and destination state of an earlier pass.
zxfer_publish_dry_run_snapshot_discovery() {
	zxfer_reset_snapshot_discovery_state
	zxfer_reset_destination_existence_cache
	g_recursive_source_list=$g_initial_source
	g_recursive_source_dataset_list=$g_initial_source
}

# Purpose: List the destination's datasets, on the -T host when one is given,
# and publish them as the recursive destination inventory.
# Usage: zxfer_collect_destination_dataset_inventory; called after the
# snapshot diff when later work reads the destination existence cache.
zxfer_collect_destination_dataset_inventory() {
	zxfer_create_temp_file_group 2 || return "$?"
	l_destination_inventory_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_dest_list_tmp_file
		IFS= read -r l_dest_list_err_file
	} <<-EOF
		$l_destination_inventory_stage_files
	EOF

	if zxfer_command_trace_enabled; then
		zxfer_trace_rendered_command "Running command" \
			"$(zxfer_render_destination_zfs_command list -t filesystem,volume -Hr -o name "$g_destination")"
	else
		zxfer_record_last_command_opaque
	fi
	l_dest_inventory_status=0
	zxfer_run_destination_zfs_cmd list -t filesystem,volume -Hr -o name "$g_destination" >"$l_dest_list_tmp_file" 2>"$l_dest_list_err_file" ||
		l_dest_inventory_status=$?

	l_status=0
	zxfer_publish_destination_dataset_inventory_from_stage \
		"$l_dest_list_tmp_file" \
		"$l_dest_list_err_file" \
		"$l_dest_inventory_status" ||
		l_status=$?
	zxfer_cleanup_runtime_artifact_path_list "$l_destination_inventory_stage_files"
	return "$l_status"
}

# Purpose: Copy a snapshot record file without the records whose dataset
# matches the -x pattern.
# Usage: zxfer_filter_snapshot_file_with_excludes INPUT OUTPUT
zxfer_filter_snapshot_file_with_excludes() {
	"${g_cmd_awk:-awk}" -v "exclude_pattern=${g_option_x_exclude_datasets:-}" \
		"$ZXFER_SNAPSHOT_RECORD_AWK" "$1" >"$2"
}

# Purpose: Print a file's lines in reverse order.
# Usage: zxfer_reverse_file_lines FILE [MAX_LINES]; defaults to 50000 lines.
# A file longer than MAX_LINES uses numbered sort, so awk memory stays bounded.
# A nonnumeric MAX_LINES selects that fallback immediately.
zxfer_reverse_file_lines() {
	l_reverse_max_lines=${2:-50000}
	zxfer_is_uint "$l_reverse_max_lines" || l_reverse_max_lines=0

	l_reverse_status=0
	# Exits 3 without printing as soon as the file outgrows the limit.
	# shellcheck disable=SC2016  # awk program should see literal $0/NR.
	"${g_cmd_awk:-awk}" -v "max_lines=$l_reverse_max_lines" '
		NR > max_lines + 0 { too_long = 1; exit 3 }
		{ line[NR] = $0 }
		END { if (!too_long) for (i = NR; i > 0; i--) print line[i] }' "$1" ||
		l_reverse_status=$?
	[ "$l_reverse_status" -eq 3 ] || return "$l_reverse_status"

	zxfer_get_temp_file || return "$?"
	l_reverse_numbered_file=$g_zxfer_temp_file_result
	l_reverse_status=0
	cat -n "$1" >"$l_reverse_numbered_file" || l_reverse_status=$?
	if [ "$l_reverse_status" -eq 0 ]; then
		# Sort on the cat -n line number, last line first, then drop it.
		LC_ALL=C sort -nr "$l_reverse_numbered_file" | cut -f2- || l_reverse_status=$?
	fi
	zxfer_cleanup_runtime_artifact_path "$l_reverse_numbered_file"
	return "$l_reverse_status"
}

# Purpose: Publish the sorted, unique dataset names of a snapshot record file.
# Usage: zxfer_capture_recursive_dataset_list_from_snapshot_file FILE SCRATCH;
# sets g_zxfer_recursive_dataset_list_result and overwrites SCRATCH.
zxfer_capture_recursive_dataset_list_from_snapshot_file() {
	g_zxfer_recursive_dataset_list_result=""
	# A record's dataset is everything before its first @.
	# shellcheck disable=SC2016  # awk program should see literal $1.
	"${g_cmd_awk:-awk}" -F@ '$1 != "" { print $1 }' "$1" >"$2" || return "$?"
	l_capture_dataset_list=$(LC_ALL=C sort -u "$2") || return "$?"
	g_zxfer_recursive_dataset_list_result=$l_capture_dataset_list
}

# Purpose: Drop the datasets that match the -x pattern from a dataset list.
# Usage: zxfer_filter_recursive_dataset_list_with_excludes LIST; sets
# g_zxfer_recursive_dataset_list_result, or returns grep's error status.
zxfer_filter_recursive_dataset_list_with_excludes() {
	g_zxfer_recursive_dataset_list_result=$1
	[ -n "$1" ] && [ -n "${g_option_x_exclude_datasets:-}" ] || return 0

	g_zxfer_recursive_dataset_list_result=""
	l_filter_status=0
	l_filter_dataset_list=$(
		grep -v -e "$g_option_x_exclude_datasets" <<-EOF
			$1
		EOF
	) || l_filter_status=$?
	# grep exits 1 when it drops every dataset; 2 is a real failure.
	[ "$l_filter_status" -le 1 ] || return "$l_filter_status"
	g_zxfer_recursive_dataset_list_result=$l_filter_dataset_list
}

# Purpose: Publish the -x filtered dataset names of one delta record file.
# Usage: zxfer_capture_delta_dataset_list FILE SCRATCH LABEL; sets
# g_zxfer_recursive_dataset_list_result, empty for an empty FILE. Failures
# throw "Failed to derive LABEL." or "Failed to filter LABEL against exclude
# patterns.".
zxfer_capture_delta_dataset_list() {
	g_zxfer_recursive_dataset_list_result=""
	[ -s "$1" ] || return 0
	zxfer_capture_recursive_dataset_list_from_snapshot_file "$1" "$2" ||
		zxfer_throw_error "Failed to derive $3." "$?"
	zxfer_filter_recursive_dataset_list_with_excludes "$g_zxfer_recursive_dataset_list_result" ||
		zxfer_throw_error "Failed to filter $3 against exclude patterns." "$?"
}

# Purpose: Decide whether recursive source dataset inventory must be derived
# from the full source snapshot list.
# Usage: zxfer_snapshot_discovery_needs_source_dataset_inventory; returns 1
# for a recursive run with no property work (-P, -o, or -U with source
# datasets to scan), which never reads the inventory.
zxfer_snapshot_discovery_needs_source_dataset_inventory() {
	if [ "${g_option_R_recursive:-}" = "" ]; then
		return 0
	fi
	if [ "${g_option_P_transfer_property:-0}" -eq 1 ] ||
		[ -n "${g_option_o_override_property:-}" ]; then
		return 0
	fi
	if [ "${g_option_U_skip_unsupported_properties:-0}" -eq 1 ] &&
		[ -n "${g_recursive_source_list:-}" ]; then
		return 0
	fi

	return 1
}

# Purpose: Split two sorted record files into the records only the source has
# and the records only the destination has.
# Usage: zxfer_write_snapshot_delta_files SOURCE DESTINATION MISSING EXTRA
# SCRATCH; writes MISSING and EXTRA in sorted order and overwrites SCRATCH.
zxfer_write_snapshot_delta_files() {
	LC_ALL=C comm -3 "$1" "$2" >"$5" || return "$?"
	# comm -3 indents destination-only records with one tab. A snapshot name
	# cannot start with a tab, so dropping exactly one keeps the record.
	# shellcheck disable=SC2016  # awk program should see literal $0.
	ZXFER_AWK_DELTA_EXTRA_FILE=$4 "${g_cmd_awk:-awk}" '
		substr($0, 1, 1) == "\t" {
			print substr($0, 2) > (ENVIRON["ZXFER_AWK_DELTA_EXTRA_FILE"])
			next
		}
		{ print }' "$5" >"$3"
}

# Purpose: Print the recursive delta summary under -v, and the delta records
# as well under -V.
# Usage: zxfer_report_recursive_snapshot_delta MISSING EXTRA; call once the
# recursive dataset lists are final.
zxfer_report_recursive_snapshot_delta() {
	l_report_delta_missing_file=$1
	l_report_delta_extra_file=$2

	if [ "${g_option_v_verbose:-0}" -ne 1 ] && [ "${g_option_V_very_verbose:-0}" -ne 1 ]; then
		return 0
	fi
	# One pass counts the records of both delta files, then the datasets of
	# both lists from stdin, split by a lone "@" (never a dataset name). The
	# lists go through a here-doc: argv and environment strings are
	# size-limited, and a large tree would overflow them.
	# shellcheck disable=SC2016  # awk program should see literal ARGV/FILENAME.
	l_report_delta_counts=$(
		"${g_cmd_awk:-awk}" '
		FILENAME == ARGV[1] { missing++; next }
		FILENAME == ARGV[2] { extra++; next }
		$0 == "@" { in_extra_list = 1; next }
		NF { if (in_extra_list) extra_datasets++; else source_datasets++ }
		END { print missing + 0, extra + 0, source_datasets + 0, extra_datasets + 0 }' \
			"$l_report_delta_missing_file" "$l_report_delta_extra_file" - <<-EOF
				$g_recursive_source_list
				@
				$g_recursive_destination_extra_dataset_list
			EOF
	) || zxfer_throw_error "Failed to count the recursive snapshot delta." "$?"
	read -r l_report_delta_missing_count l_report_delta_extra_count \
		l_report_delta_source_dataset_count l_report_delta_extra_dataset_count <<-EOF
			$l_report_delta_counts
		EOF

	if [ "${l_report_delta_missing_count:-0}" -gt 0 ] || [ "${l_report_delta_extra_count:-0}" -gt 0 ]; then
		zxfer_echov "Recursive snapshot delta summary: source_missing_snapshots=$l_report_delta_missing_count destination_extra_snapshots=$l_report_delta_extra_count source_datasets=$l_report_delta_source_dataset_count destination_extra_datasets=$l_report_delta_extra_dataset_count"
		if [ -n "$g_recursive_source_list" ]; then
			zxfer_echov "Recursive source datasets queued for transfer:"
			printf '%s\n' "$g_recursive_source_list" | while IFS= read -r l_report_delta_source_dataset; do
				[ -n "$l_report_delta_source_dataset" ] || continue
				zxfer_echov "  $l_report_delta_source_dataset"
			done
		fi
		if [ -n "$g_recursive_destination_extra_dataset_list" ]; then
			zxfer_echov "Recursive destination datasets queued for delete inspection:"
			printf '%s\n' "$g_recursive_destination_extra_dataset_list" | while IFS= read -r l_report_delta_destination_dataset; do
				[ -n "$l_report_delta_destination_dataset" ] || continue
				zxfer_echov "  $l_report_delta_destination_dataset"
			done
		fi
	fi

	[ "${g_option_V_very_verbose:-0}" -eq 1 ] || return 0
	echo "====================================================================="
	echo "====== Snapshots present in source but missing in destination ======"
	[ -s "$l_report_delta_missing_file" ] && cat "$l_report_delta_missing_file"
	echo "====== Source datasets that differ from destination ======"
	echo "g_recursive_source_list:"
	echo "$g_recursive_source_list"
	echo "Source dataset count: $l_report_delta_source_dataset_count"
	echo "====================================================================="
	echo "====== Extra Destination snapshots not in source ======"
	[ -s "$l_report_delta_extra_file" ] && cat "$l_report_delta_extra_file"
	echo "====== Destination datasets with extra snapshots not in source ======"
	if [ "$g_recursive_destination_extra_dataset_list" != "" ]; then
		printf '%s\n' "$g_recursive_destination_extra_dataset_list"
	fi
	echo "====================================================================="
}

# Purpose: Diff the source and destination snapshot listings and publish the
# recursive work lists.
# Usage: zxfer_set_g_recursive_source_list RAW_SOURCE SORTED_DESTINATION
# [SORTED_SOURCE]; RAW_SOURCE is sorted here unless SORTED_SOURCE is given.
# Publishes g_recursive_source_list, g_recursive_destination_extra_dataset_list
# and, when later work needs it, g_recursive_source_dataset_list, all without
# -x matches. Recursive -d work excludes datasets absent from a successful
# live source dataset inventory, preserving target-only datasets and snapshots.
# Failures throw.
zxfer_set_g_recursive_source_list() {
	l_delta_raw_source_file=$1
	l_delta_destination_file=$2
	l_delta_source_file=${3:-}

	zxfer_create_temp_file_group 6 || return "$?"
	l_delta_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_delta_sorted_source_file
		IFS= read -r l_delta_missing_file
		IFS= read -r l_delta_extra_file
		IFS= read -r l_delta_filtered_source_file
		IFS= read -r l_delta_filtered_destination_file
		IFS= read -r l_delta_scratch_file
	} <<-EOF
		$l_delta_stage_files
	EOF

	if [ -z "$l_delta_source_file" ]; then
		l_delta_source_file=$l_delta_sorted_source_file
		if zxfer_command_trace_enabled; then
			zxfer_trace_rendered_command "Running command" \
				"$(zxfer_render_command_for_report "LC_ALL=C" sort "$l_delta_raw_source_file") > $(zxfer_quote_token_for_report "$l_delta_source_file")"
		else
			zxfer_record_last_command_opaque
		fi
		LC_ALL=C sort "$l_delta_raw_source_file" >"$l_delta_source_file" ||
			zxfer_throw_error "Failed to sort source snapshots for recursive delta planning." "$?"
	elif [ ! -f "$l_delta_source_file" ]; then
		zxfer_throw_error "Failed to locate staged sorted source snapshots for recursive delta planning."
	fi

	# -x first drops excluded records, so differences confined to excluded
	# datasets cannot keep a run from comparing as a no-op.
	if [ -n "${g_option_x_exclude_datasets:-}" ]; then
		zxfer_filter_snapshot_file_with_excludes "$l_delta_source_file" "$l_delta_filtered_source_file" ||
			zxfer_throw_error "Failed to filter source snapshots against exclude patterns for recursive delta planning." "$?"
		zxfer_filter_snapshot_file_with_excludes "$l_delta_destination_file" "$l_delta_filtered_destination_file" ||
			zxfer_throw_error "Failed to filter destination snapshots against exclude patterns for recursive delta planning." "$?"
		l_delta_source_file=$l_delta_filtered_source_file
		l_delta_destination_file=$l_delta_filtered_destination_file
	fi

	# Equal listings leave both freshly allocated delta files empty.
	l_delta_compare_status=0
	cmp -s "$l_delta_source_file" "$l_delta_destination_file" || l_delta_compare_status=$?
	case $l_delta_compare_status in
	0) ;;
	1)
		zxfer_write_snapshot_delta_files "$l_delta_source_file" "$l_delta_destination_file" \
			"$l_delta_missing_file" "$l_delta_extra_file" "$l_delta_scratch_file" ||
			zxfer_throw_error "Failed to diff source and destination snapshots for recursive delta planning." "$?"
		;;
	*)
		zxfer_throw_error "Failed to compare source and destination snapshots for recursive delta planning." "$l_delta_compare_status"
		;;
	esac

	zxfer_capture_delta_dataset_list "$l_delta_missing_file" "$l_delta_scratch_file" \
		"recursive source dataset transfer list"
	g_recursive_source_list=$g_zxfer_recursive_dataset_list_result
	zxfer_capture_delta_dataset_list "$l_delta_extra_file" "$l_delta_scratch_file" \
		"recursive destination dataset delete list"
	g_recursive_destination_extra_dataset_list=$g_zxfer_recursive_dataset_list_result
	if [ -n "${g_option_R_recursive:-}" ] &&
		[ "$g_option_d_delete_destination_snapshots" -eq 1 ] &&
		[ -n "$g_recursive_destination_extra_dataset_list" ]; then
		# Snapshot rows cannot distinguish an absent dataset from an existing
		# dataset with no snapshots. List datasets before admitting -d work.
		zxfer_run_source_zfs_cmd list -Hr -t filesystem,volume -o name "$g_initial_source" \
			>"$l_delta_scratch_file" ||
			zxfer_throw_error "Failed to retrieve source dataset inventory for recursive destination snapshot cleanup." "$?"
		# Validate the root before trusting any absence, then label each extra
		# dataset for retention or a notice through the reporting helper.
		# shellcheck disable=SC2016 # awk reads literal fields and ENVIRON.
		l_delta_delete_dataset_rows=$(
			ZXFER_AWK_INITIAL_SOURCE=$g_initial_source
			export ZXFER_AWK_INITIAL_SOURCE
			"${g_cmd_awk:-awk}" '
				side == "source" {
					if ($0 == ENVIRON["ZXFER_AWK_INITIAL_SOURCE"]) root_seen = 1
					source[$0] = 1
					next
				}
				NF { print ($0 in source ? "keep" : "skip") "\t" $0 }
				END { if (!root_seen) exit 1 }
			' side=source "$l_delta_scratch_file" side=extra - <<EOF
$g_recursive_destination_extra_dataset_list
EOF
		) || zxfer_throw_error "Failed to filter recursive destination snapshot cleanup against a complete source dataset inventory." "$?"
		g_recursive_destination_extra_dataset_list=""
		while IFS="$ZXFER_TAB" read -r l_delta_delete_action l_delta_delete_dataset; do
			if [ "$l_delta_delete_action" = keep ]; then
				g_recursive_destination_extra_dataset_list=${g_recursive_destination_extra_dataset_list:+$g_recursive_destination_extra_dataset_list$ZXFER_LF}$l_delta_delete_dataset
			else
				zxfer_map_destination_dataset "$l_delta_delete_dataset"
				zxfer_warn_stderr "zxfer: destination dataset [$g_zxfer_destination_dataset_result] has no source counterpart; leaving it and its snapshots untouched."
			fi
		done <<EOF
$l_delta_delete_dataset_rows
EOF
	fi
	g_recursive_source_dataset_list=""
	if zxfer_snapshot_discovery_needs_source_dataset_inventory; then
		zxfer_capture_delta_dataset_list "$l_delta_source_file" "$l_delta_scratch_file" \
			"recursive source dataset inventory"
		g_recursive_source_dataset_list=$g_zxfer_recursive_dataset_list_result
	fi

	zxfer_report_recursive_snapshot_delta "$l_delta_missing_file" "$l_delta_extra_file"
	if [ "$g_recursive_source_list" = "" ]; then
		zxfer_echov "No new snapshots to transfer."
	fi
	zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
}

# Purpose: Decide whether snapshot discovery must keep per-dataset record
# caches for later replication work.
# Usage: Called after recursive snapshot diffing has populated the dataset
# work lists, before discovery decides whether to carry large snapshot
# inventories forward.
zxfer_snapshot_discovery_needs_record_caches() {
	if [ "${g_option_R_recursive:-}" = "" ]; then
		return 0
	fi
	if [ -n "${g_recursive_source_list:-}" ]; then
		return 0
	fi
	if [ "${g_option_d_delete_destination_snapshots:-0}" -eq 1 ] &&
		[ -n "${g_recursive_destination_extra_dataset_list:-}" ]; then
		return 0
	fi
	if [ "${g_option_P_transfer_property:-0}" -eq 1 ] ||
		[ -n "${g_option_o_override_property:-}" ]; then
		return 0
	fi

	return 1
}

# Purpose: Decide whether the recursive destination dataset inventory should
# be collected after snapshot diffing.
# Usage: Called after recursive snapshot deltas are known so no-op runs avoid
# building a destination existence cache that no later stage can consume; the
# conditions mirror the work zxfer_copy_filesystems skips.
zxfer_snapshot_discovery_needs_destination_dataset_inventory() {
	if [ -n "${g_recursive_source_list:-}" ]; then
		return 0
	fi
	if [ "${g_option_d_delete_destination_snapshots:-0}" -eq 1 ] &&
		[ -n "${g_recursive_destination_extra_dataset_list:-}" ]; then
		return 0
	fi
	if [ "${g_option_P_transfer_property:-0}" -eq 1 ] ||
		[ -n "${g_option_o_override_property:-}" ]; then
		return 0
	fi

	return 1
}

# Purpose: Start the source producer for a fast recursive no-op proof attempt.
# Usage: zxfer_start_fast_recursive_noop_source_discovery STREAM ERROR COUNT;
# publishes the registered producer in g_last_background_pid and keeps its
# rendered command in g_source_snapshot_list_cmd for failure reports.
zxfer_start_fast_recursive_noop_source_discovery() {
	zxfer_build_source_snapshot_name_list_cmd || return "$?"
	l_fast_noop_source_start_command=$g_zxfer_source_snapshot_list_cmd_result

	l_fast_noop_source_start_uses_parallel=0
	if [ "${g_source_snapshot_list_uses_parallel:-0}" -eq 1 ]; then
		l_fast_noop_source_start_uses_parallel=1
	fi
	g_source_snapshot_list_cmd=$l_fast_noop_source_start_command
	g_zxfer_profile_source_snapshot_list_commands=$((g_zxfer_profile_source_snapshot_list_commands + 1))
	if [ "$l_fast_noop_source_start_uses_parallel" -eq 1 ]; then
		g_zxfer_profile_source_snapshot_list_parallel_commands=$((g_zxfer_profile_source_snapshot_list_parallel_commands + 1))
	fi
	if [ "$g_option_O_origin_host" != "" ]; then
		g_zxfer_profile_source_ssh_shell_invocations=$((g_zxfer_profile_source_ssh_shell_invocations + 1))
	fi

	# Stage into regular temp files. FIFO comparisons can strand a producer
	# when the compare command exits or cannot open both streams.
	zxfer_execute_source_snapshot_name_list_background_sort_cmd \
		"$l_fast_noop_source_start_command" \
		"$1" "$2" "$3" || return "$?"
	return 0
}

# Purpose: Start the destination producer for a fast recursive no-op proof.
# Usage: zxfer_start_fast_recursive_noop_destination_discovery STREAM ERROR
# STATUS RAW SOURCE_PID; starts after the source so both listings overlap.
# On failure, stops and reaps SOURCE_PID and returns the original start status.
zxfer_start_fast_recursive_noop_destination_discovery() {
	zxfer_start_destination_snapshot_name_sorted_fifo_producer \
		"$1" "$2" "$3" "$4" || {
		l_fast_noop_destination_start_status=$?
		l_fast_noop_source_pid=$5
		# Signal the source producer only while it is unreaped: TERM, a grace
		# period for stages that ignore it, KILL, then wait. A failed signal
		# leaves its scope registered for trap cleanup. The destination setup
		# status is the one reported.
		if zxfer_abort_fast_noop_background_pid "$l_fast_noop_source_pid" \
			"background source snapshot no-op proof helper"; then
			zxfer_cleanup_pid_abort_grace_wait
			if zxfer_abort_cleanup_pid "$l_fast_noop_source_pid" KILL; then
				wait "$l_fast_noop_source_pid" 2>/dev/null || :
				zxfer_unregister_cleanup_pid "$l_fast_noop_source_pid"
				g_last_background_pid=""
			fi
		fi
		return "$l_fast_noop_destination_start_status"
	}
	return 0
}

# Purpose: Stop the pipeline stages a failed source producer left in its
# process group after wait reaped the producer.
# Usage: zxfer_kill_reaped_producer_group PID
zxfer_kill_reaped_producer_group() {
	# Never signal a reaped PID: its number may already belong to an unrelated
	# process. A process group outlives its leader, so a pgid-scoped producer's
	# group is still reachable. Stages that a wrapper-scoped producer left
	# behind were reparented and cannot be stopped from here.
	zxfer_find_cleanup_pid_record "$1" || return 0
	[ "$g_zxfer_cleanup_pid_record_scope" = pgid ] || return 0
	zxfer_signal_process_group KILL "$1" || :
}

# Purpose: Reap a discovery producer and account for its elapsed listing time.
# Usage: zxfer_wait_for_snapshot_discovery_producer PID START_MS ROLE; ROLE is
# source or destination. Returns the exact wait status. A failed source's
# surviving process group is stopped before its cleanup registration is removed.
zxfer_wait_for_snapshot_discovery_producer() {
	l_discovery_producer_wait_status=0
	[ -z "$1" ] || wait "$1" || l_discovery_producer_wait_status=$?
	if [ "$3" = source ] && [ "$l_discovery_producer_wait_status" -ne 0 ]; then
		zxfer_kill_reaped_producer_group "$1"
	fi
	[ -z "$1" ] || zxfer_unregister_cleanup_pid "$1"
	zxfer_profile_stop_timer "$2"
	case $3 in
	source)
		g_zxfer_profile_source_snapshot_listing_ms=$((g_zxfer_profile_source_snapshot_listing_ms + g_zxfer_profile_elapsed_ms))
		;;
	destination)
		g_zxfer_profile_destination_snapshot_listing_ms=$((g_zxfer_profile_destination_snapshot_listing_ms + g_zxfer_profile_elapsed_ms))
		;;
	esac
	return "$l_discovery_producer_wait_status"
}

# Purpose: Compare the completed fast recursive no-op proof streams.
# Usage: zxfer_compare_fast_recursive_noop_discovery_streams SOURCE DEST COMPARE;
# called before
# sidecar validation, so a real delta declines the proof (returns 1) at once.
# Returns: Other failures publish g_zxfer_snapshot_discovery_failure_result;
# the operation owner releases stages before reporting them.
zxfer_compare_fast_recursive_noop_discovery_streams() {
	zxfer_profile_start_timer
	l_fast_noop_compare_stage_start_ms=$g_zxfer_profile_clock_ms
	# Any line comm prints is a snapshot that differs.
	l_fast_noop_compare_status=0
	if LC_ALL=C comm -3 \
		"$1" "$2" >"$3"; then
		if [ -s "$3" ]; then
			l_fast_noop_compare_status=1
		fi
	else
		l_fast_noop_compare_status=$?
	fi
	zxfer_profile_stop_timer "$l_fast_noop_compare_stage_start_ms"
	g_zxfer_profile_snapshot_diff_sort_ms=$((g_zxfer_profile_snapshot_diff_sort_ms + g_zxfer_profile_elapsed_ms))

	if [ "$l_fast_noop_compare_status" -ne 0 ]; then
		if [ "$l_fast_noop_compare_status" -eq 1 ]; then
			zxfer_reset_destination_existence_cache
			return 1
		fi
		g_zxfer_snapshot_discovery_failure_result="Failed to compare source and destination snapshots for recursive no-op proof."
		return "$l_fast_noop_compare_status"
	fi
	return 0
}

# Purpose: Validate the destination producer's statuses for a fast no-op proof.
# Usage: zxfer_validate_fast_recursive_noop_destination_discovery STATUS_READ
# LIST_STATUS NORMALIZE_STATUS SORT_STATUS WAIT_STATUS ERROR_FILE; called after
# the streams compare equal. A validated nonzero LIST_STATUS means the dataset
# is missing. Other failures publish their diagnostic for the operation owner.
zxfer_validate_fast_recursive_noop_destination_discovery() {
	[ "$1" -eq 0 ] || {
		g_zxfer_snapshot_discovery_failure_result="Failed to validate destination snapshot status for recursive no-op proof."
		return 1
	}

	l_fast_noop_destination_list_status=$2
	if [ "$l_fast_noop_destination_list_status" -ne 0 ]; then
		zxfer_read_snapshot_discovery_capture_file \
			"$6" || {
			l_fast_noop_destination_error_read_status=$?
			g_zxfer_snapshot_discovery_failure_result="Failed to read staged destination snapshot stderr."
			return "$l_fast_noop_destination_error_read_status"
		}
		l_fast_noop_destination_error=$g_zxfer_snapshot_discovery_file_read_result
		if zxfer_destination_probe_reports_missing "$l_fast_noop_destination_error"; then
			: # A failed listing with this diagnostic is a valid missing dataset.
		else
			if [ -n "$l_fast_noop_destination_error" ]; then
				printf '%s\n' "$l_fast_noop_destination_error" >&2
			fi
			g_zxfer_snapshot_discovery_failure_result="Failed to retrieve snapshot list from the destination."
			return "$l_fast_noop_destination_list_status"
		fi
	fi

	for l_fast_noop_destination_status in \
		"$3" "$4" "$5"; do
		[ "$l_fast_noop_destination_status" -eq 0 ] && continue
		return "$l_fast_noop_destination_status"
	done
	return 0
}

# Purpose: Validate source stderr, wait status, and count sidecar for a fast proof.
# Usage: zxfer_validate_fast_recursive_noop_source_discovery WAIT_STATUS ERROR
# COUNT; called after destination validation. Preserves exact source diagnostics
# and the exclusion-specific empty-source fallback. Failures return the status
# with their diagnostic in g_zxfer_snapshot_discovery_failure_result.
zxfer_validate_fast_recursive_noop_source_discovery() {
	if [ "$1" -ne 0 ]; then
		if [ -n "${g_source_snapshot_list_cmd:-}" ]; then
			zxfer_record_last_command_string "$g_source_snapshot_list_cmd"
		fi
		zxfer_read_snapshot_discovery_capture_file \
			"$2" || {
			l_fast_noop_source_error_read_status=$?
			g_zxfer_snapshot_discovery_failure_result="Failed to read staged source snapshot stderr."
			return "$l_fast_noop_source_error_read_status"
		}
		l_fast_noop_source_error=$g_zxfer_snapshot_discovery_file_read_result
		l_fast_noop_source_error=$(zxfer_limit_snapshot_discovery_capture_lines \
			"$l_fast_noop_source_error" 10)
		l_fast_noop_source_wait_status=$1
		g_zxfer_snapshot_discovery_failure_result="Failed to retrieve snapshots from the source${l_fast_noop_source_error:+: $l_fast_noop_source_error}"
		return "$l_fast_noop_source_wait_status"
	fi

	l_fast_noop_source_count_status=0
	zxfer_read_snapshot_discovery_status_file \
		"$3" 1 ||
		l_fast_noop_source_count_status=$?
	l_fast_noop_source_snapshot_count=$g_zxfer_snapshot_discovery_status_file_result
	if [ "$l_fast_noop_source_count_status" -ne 0 ] ||
		[ "$l_fast_noop_source_snapshot_count" -ne 1 ]; then
		if [ -n "${g_option_x_exclude_datasets:-}" ]; then
			zxfer_reset_destination_existence_cache
			return 1
		fi
		g_zxfer_snapshot_discovery_failure_result="Failed to retrieve snapshots from the source"
		return 1
	fi
	return 0
}

# Purpose: Validate the two completed producers and reuse a successful raw listing.
# Usage: zxfer_validate_fast_recursive_noop_discovery SOURCE DEST SOURCE_ERROR
# COMPARE COUNT DEST_ERROR STATUS RAW SOURCE_WAIT DEST_WAIT. Files belong to
# the calling operation; only the validated RAW handoff is shared with full
# discovery. Read the status record once before comparison, preserving the
# delta-first fallback even when a producer failed.
zxfer_validate_fast_recursive_noop_discovery() {
	l_fast_noop_status_read_status=1
	l_fast_noop_list_status=""
	l_fast_noop_normalize_status=""
	l_fast_noop_sort_status=""
	if [ -f "$7" ]; then
		read -r l_fast_noop_list_status l_fast_noop_normalize_status \
			l_fast_noop_sort_status <"$7" || :
		if zxfer_is_uint "$l_fast_noop_list_status" &&
			zxfer_is_uint "$l_fast_noop_normalize_status" &&
			zxfer_is_uint "$l_fast_noop_sort_status"; then
			l_fast_noop_status_read_status=0
			if [ "$l_fast_noop_list_status" -eq 0 ]; then
				g_zxfer_snapshot_discovery_destination_listing_file=$8
			fi
		fi
	fi
	zxfer_compare_fast_recursive_noop_discovery_streams "$1" "$2" "$4" || return "$?"
	zxfer_validate_fast_recursive_noop_destination_discovery \
		"$l_fast_noop_status_read_status" "$l_fast_noop_list_status" \
		"$l_fast_noop_normalize_status" "$l_fast_noop_sort_status" "${10}" "$6" || return "$?"
	zxfer_validate_fast_recursive_noop_source_discovery "$9" "$3" "$5" || return "$?"

	if [ "$l_fast_noop_list_status" -ne 0 ]; then
		zxfer_map_destination_dataset
		zxfer_echoV "Destination dataset does not exist: $g_zxfer_destination_dataset_result"
		zxfer_reset_destination_existence_cache
		return 1
	fi
	return 0
}

# Purpose: Overlap and reap the fast proof's producers with explicit ownership.
# Usage: zxfer_run_fast_recursive_noop_discovery SOURCE DEST SOURCE_ERROR
# COMPARE COUNT DEST_ERROR STATUS RAW. The eight owned paths stay in positional
# parameters; append each timer and PID immediately before another stage can
# overwrite the shared profiling clock or background-PID result channel.
zxfer_run_fast_recursive_noop_discovery() {
	zxfer_profile_start_timer
	set -- "$@" "$g_zxfer_profile_clock_ms"
	zxfer_start_fast_recursive_noop_source_discovery "$1" "$3" "$5" || return "$?"
	set -- "$@" "$g_last_background_pid"
	zxfer_profile_start_timer
	set -- "$@" "$g_zxfer_profile_clock_ms"
	zxfer_start_fast_recursive_noop_destination_discovery \
		"$2" "$6" "$7" "$8" "${10}" || return "$?"
	set -- "$@" "$g_last_background_pid"

	l_fast_noop_source_status=0
	zxfer_wait_for_snapshot_discovery_producer "${10}" "$9" source || l_fast_noop_source_status=$?
	l_fast_noop_destination_status=0
	zxfer_wait_for_snapshot_discovery_producer "${12}" "${11}" destination || l_fast_noop_destination_status=$?
	g_last_background_pid=""
	zxfer_validate_fast_recursive_noop_discovery \
		"$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" \
		"$l_fast_noop_source_status" "$l_fast_noop_destination_status" &&
		zxfer_publish_fast_recursive_noop_discovery
}

# Purpose: Publish a proven fast recursive no-op result.
# Usage: zxfer_publish_fast_recursive_noop_discovery; called only after all
# validation succeeds.
zxfer_publish_fast_recursive_noop_discovery() {
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_snapshot_discovery_destination_listing_file=""
	zxfer_echov "No new snapshots to transfer."
	return 0
}

# Purpose: Try to prove a clean recursive no-op (local or remote-origin source)
# with identity-aware discovery before the full creation-order source listing.
# Usage: zxfer_try_fast_recursive_noop_discovery; returns 0 when the no-op is
# proven, 1 when full discovery should run, other statuses on failure.
zxfer_try_fast_recursive_noop_discovery() {
	zxfer_fast_recursive_noop_options_are_eligible || return 1
	g_zxfer_snapshot_discovery_failure_result=""
	zxfer_get_temp_file || return "$?"
	# Keep the raw handoff separate from the seven proof-only files. Function
	# arguments, unlike l_* assignments in POSIX sh, survive child helper calls.
	set -- "$g_zxfer_temp_file_result"
	l_fast_noop_operation_status=0
	zxfer_create_temp_file_group 7 || l_fast_noop_operation_status=$?
	if [ "$l_fast_noop_operation_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$1"
		return "$l_fast_noop_operation_status"
	fi
	{
		IFS= read -r l_fast_noop_source_file
		IFS= read -r l_fast_noop_destination_file
		IFS= read -r l_fast_noop_source_error_file
		IFS= read -r l_fast_noop_compare_file
		IFS= read -r l_fast_noop_count_file
		IFS= read -r l_fast_noop_destination_error_file
		IFS= read -r l_fast_noop_status_file
	} <<-EOF
		$g_zxfer_temp_file_group_result
	EOF
	set -- "$l_fast_noop_source_file" "$l_fast_noop_destination_file" \
		"$l_fast_noop_source_error_file" "$l_fast_noop_compare_file" \
		"$l_fast_noop_count_file" "$l_fast_noop_destination_error_file" \
		"$l_fast_noop_status_file" "$1"
	unset l_fast_noop_source_file l_fast_noop_destination_file l_fast_noop_source_error_file \
		l_fast_noop_compare_file l_fast_noop_count_file l_fast_noop_destination_error_file l_fast_noop_status_file
	zxfer_run_fast_recursive_noop_discovery "$@" || l_fast_noop_operation_status=$?
	if [ "$l_fast_noop_operation_status" -ne 0 ]; then
		# Success ends the pass: EXIT removes its private root. On fallback or
		# failure, release all scratch except the validated raw listing handoff.
		l_fast_noop_cleanup_files=$1$ZXFER_LF$2$ZXFER_LF$3$ZXFER_LF$4$ZXFER_LF$5$ZXFER_LF$6$ZXFER_LF$7
		[ "$8" = "${g_zxfer_snapshot_discovery_destination_listing_file:-}" ] ||
			l_fast_noop_cleanup_files=$l_fast_noop_cleanup_files$ZXFER_LF$8
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_noop_cleanup_files"
	fi
	[ -z "$g_zxfer_snapshot_discovery_failure_result" ] ||
		zxfer_throw_error "$g_zxfer_snapshot_discovery_failure_result" "$l_fast_noop_operation_status"
	return "$l_fast_noop_operation_status"
}

# Purpose: Collect and normalize the destination side of full discovery.
# Usage: zxfer_collect_full_destination_snapshot_discovery RAW SORTED REUSE;
# runs while the source listing is in flight. REUSE=1 normalizes the validated
# listing handed over by a declined fast proof instead of listing again.
# Returns: Zero after normalization, otherwise the original helper status.
zxfer_collect_full_destination_snapshot_discovery() {
	zxfer_profile_start_timer
	l_full_destination_stage_start_ms=$g_zxfer_profile_clock_ms
	zxfer_map_destination_dataset
	l_full_destination_dataset=$g_zxfer_destination_dataset_result
	l_full_destination_status=0
	if [ "$3" -eq 1 ]; then
		# The fast proof's listing succeeded, so the root exists.
		zxfer_set_destination_existence_cache_entry "$l_full_destination_dataset" 1
		zxfer_normalize_destination_snapshot_list "$l_full_destination_dataset" \
			"$1" "$2" ||
			l_full_destination_status=$?
	else
		zxfer_write_destination_snapshot_list_to_files \
			"$1" "$2" ||
			l_full_destination_status=$?
	fi
	[ "$l_full_destination_status" -eq 0 ] || return "$l_full_destination_status"

	zxfer_profile_stop_timer "$l_full_destination_stage_start_ms"
	g_zxfer_profile_destination_snapshot_listing_ms=$((g_zxfer_profile_destination_snapshot_listing_ms + g_zxfer_profile_elapsed_ms))
	return 0
}

# Purpose: Wait for and validate the full source snapshot producer.
# Usage: zxfer_wait_for_full_source_snapshot_discovery SOURCE ERROR START_MS;
# called after the destination side is collected, so both sides overlap.
# Returns: Zero when source staging is complete, otherwise the original status
# with its diagnostic in g_zxfer_snapshot_discovery_failure_result.
zxfer_wait_for_full_source_snapshot_discovery() {
	zxfer_echoV "Waiting for background processes to finish."
	l_full_source_wait_status=0
	zxfer_wait_for_snapshot_discovery_producer "${g_source_snapshot_list_pid:-}" \
		"$3" source || l_full_source_wait_status=$?
	g_source_snapshot_list_pid=""

	if [ "$l_full_source_wait_status" -ne 0 ]; then
		if [ -n "${g_source_snapshot_list_cmd:-}" ]; then
			zxfer_record_last_command_string "$g_source_snapshot_list_cmd"
		fi
		zxfer_read_snapshot_discovery_capture_file \
			"$2" || {
			l_full_source_stderr_read_status=$?
			g_zxfer_snapshot_discovery_failure_result="Failed to read staged source snapshot stderr."
			return "$l_full_source_stderr_read_status"
		}
		l_full_source_snapshot_error=$g_zxfer_snapshot_discovery_file_read_result
		l_full_source_snapshot_error=$(zxfer_limit_snapshot_discovery_capture_lines \
			"$l_full_source_snapshot_error" 10)
		g_zxfer_snapshot_discovery_failure_result="Failed to retrieve snapshots from the source${l_full_source_snapshot_error:+: $l_full_source_snapshot_error}"
		return "$l_full_source_wait_status"
	fi
	zxfer_echoV "Background processes finished."

	if [ ! -s "$1" ]; then
		g_zxfer_snapshot_discovery_failure_result="Failed to retrieve snapshots from the source"
		return 1
	fi
	return 0
}

# Purpose: Publish the full discovery deltas, the destination dataset
# inventory and the record caches that later planning needs.
# Usage: zxfer_publish_full_snapshot_discovery_results SOURCE SOURCE_SORTED
# DEST DEST_SORTED; the last stage, after both producers finish. The owner
# releases transient listings; the record caches stay for planning.
# Returns: Zero after publication, otherwise the original helper status.
zxfer_publish_full_snapshot_discovery_results() {
	l_full_publish_inventory_attempted=0
	zxfer_profile_start_timer
	l_full_publish_diff_start_ms=$g_zxfer_profile_clock_ms
	l_full_publish_status=0
	zxfer_set_g_recursive_source_list \
		"$1" "$4" "$2" ||
		l_full_publish_status=$?
	zxfer_profile_stop_timer "$l_full_publish_diff_start_ms"
	g_zxfer_profile_snapshot_diff_sort_ms=$((g_zxfer_profile_snapshot_diff_sort_ms + g_zxfer_profile_elapsed_ms))

	if [ "$l_full_publish_status" -eq 0 ] &&
		zxfer_snapshot_discovery_needs_destination_dataset_inventory; then
		zxfer_collect_destination_dataset_inventory || l_full_publish_status=$?
		l_full_publish_inventory_attempted=1
	fi

	if [ "$l_full_publish_status" -eq 0 ] && zxfer_snapshot_discovery_needs_record_caches; then
		zxfer_publish_full_snapshot_record_caches "$1" "$3" || l_full_publish_status=$?
	fi
	[ "$l_full_publish_status" -eq 0 ] || return "$l_full_publish_status"

	if [ "$l_full_publish_inventory_attempted" -eq 1 ] &&
		[ "$g_recursive_dest_list" = "" ]; then
		zxfer_echoV "Destination dataset list is empty; assuming no existing datasets under \"$g_destination\""
	fi
	return 0
}

# Purpose: Release full discovery's transient files after publication or failure.
# Usage: zxfer_cleanup_full_snapshot_discovery_operation_state STATUS SOURCE
# ERROR SOURCE_SORTED DEST DEST_SORTED; retains
# published record caches only on success. An unreaped source producer keeps
# its files until the owner's immediate failure throw: the EXIT trap stops
# registered producers before removing the run root. Lower-level throws use
# that same teardown order.
zxfer_cleanup_full_snapshot_discovery_operation_state() {
	[ -z "${g_source_snapshot_list_pid:-}" ] || return 0
	l_full_cleanup_files=$2$ZXFER_LF$3$ZXFER_LF$4$ZXFER_LF$6
	if [ "$1" -ne 0 ] ||
		[ "$5" != "${g_zxfer_destination_snapshot_record_cache_file:-}" ]; then
		l_full_cleanup_files=$l_full_cleanup_files$ZXFER_LF$5
	fi
	zxfer_cleanup_runtime_artifact_path_list "$l_full_cleanup_files"
	[ "$1" -eq 0 ] || zxfer_cleanup_snapshot_record_cache_files
}

# Purpose: Keep the full listings as the record caches that per-dataset
# planning reads: the destination listing as is, the source listing reversed
# to newest first.
# Usage: zxfer_publish_full_snapshot_record_caches SOURCE DEST; publishes
# g_zxfer_destination_snapshot_record_cache_file and
# g_zxfer_source_snapshot_record_cache_file.
zxfer_publish_full_snapshot_record_caches() {
	g_zxfer_destination_snapshot_record_cache_file=$2
	zxfer_get_temp_file || return "$?"
	g_zxfer_source_snapshot_record_cache_file=$g_zxfer_temp_file_result
	if zxfer_command_trace_enabled; then
		zxfer_trace_rendered_command "Running command" \
			"$(zxfer_render_command_for_report "" zxfer_reverse_file_lines "$1") > $(zxfer_quote_token_for_report "$g_zxfer_source_snapshot_record_cache_file")"
	else
		zxfer_record_last_command_opaque
	fi
	zxfer_reverse_file_lines "$1" \
		>"$g_zxfer_source_snapshot_record_cache_file" ||
		zxfer_throw_error "Failed to stage source snapshot record cache." "$?"
}

# Purpose: Own full discovery's files from allocation through final release.
# Usage: zxfer_run_full_snapshot_discovery; source and destination stages
# overlap, then validation precedes publication. Positional parameters retain
# the five file paths and source timer across calls; only validated record
# caches and the registered source PID are shared beyond this operation.
zxfer_run_full_snapshot_discovery() {
	zxfer_create_temp_file_group 3 || return "$?"
	{
		IFS= read -r l_full_operation_source_file
		IFS= read -r l_full_operation_error_file
		IFS= read -r l_full_operation_sorted_file
	} <<-EOF
		$g_zxfer_temp_file_group_result
	EOF
	set -- "$l_full_operation_source_file" "$l_full_operation_error_file" \
		"$l_full_operation_sorted_file" "" ""
	unset l_full_operation_source_file l_full_operation_error_file l_full_operation_sorted_file
	g_source_snapshot_list_pid=""
	zxfer_profile_start_timer
	set -- "$@" "$g_zxfer_profile_clock_ms"
	l_full_operation_status=0
	zxfer_write_source_snapshot_list_to_file "$1" "$2" "$3" || l_full_operation_status=$?
	if [ "$l_full_operation_status" -eq 0 ]; then
		l_full_operation_reuse=0
		if [ -n "${g_zxfer_snapshot_discovery_destination_listing_file:-}" ]; then
			set -- "$1" "$2" "$3" "$g_zxfer_snapshot_discovery_destination_listing_file" "$5" "$6"
			g_zxfer_snapshot_discovery_destination_listing_file=""
			l_full_operation_reuse=1
		elif zxfer_get_temp_file; then
			set -- "$1" "$2" "$3" "$g_zxfer_temp_file_result" "$5" "$6"
		else
			l_full_operation_status=$?
		fi
		if [ "$l_full_operation_status" -eq 0 ]; then
			if zxfer_get_temp_file; then
				set -- "$1" "$2" "$3" "$4" "$g_zxfer_temp_file_result" "$6"
			else
				l_full_operation_status=$?
			fi
		fi
		if [ "$l_full_operation_status" -eq 0 ]; then
			zxfer_collect_full_destination_snapshot_discovery "$4" "$5" "$l_full_operation_reuse" &&
				zxfer_wait_for_full_source_snapshot_discovery "$1" "$2" "$6" &&
				zxfer_publish_full_snapshot_discovery_results "$1" "$3" "$4" "$5" ||
				l_full_operation_status=$?
		fi
	fi
	zxfer_cleanup_full_snapshot_discovery_operation_state "$l_full_operation_status" \
		"$1" "$2" "$3" "$4" "$5"
	return "$l_full_operation_status"
}

# Purpose: Build the source and destination snapshot inventories that the rest
# of replication planning depends on.
# Usage: zxfer_get_zfs_list; called at the start of each live pass. It throws
# on any failure, because its callers do not check its status.
#
# zxfer relies on `zfs list` in machine-readable mode (`-H`), recursive dataset
# traversal (`-r`) where needed, identity-aware output during initial discovery
# (`-o name,guid`), snapshot-only listing (`-t snapshot`), and creation-order
# sorting for per-dataset snapshot discovery on the source side. The fast
# recursive no-op proof uses the same identity-aware records without the
# creation-order sort so equal snapshot names with different GUIDs fall back to
# full discovery instead of being treated as clean.
zxfer_get_zfs_list() {
	zxfer_set_failure_stage "snapshot discovery"
	zxfer_echoV "Begin zxfer_get_zfs_list()"
	zxfer_reset_snapshot_discovery_state
	zxfer_reset_destination_existence_cache

	l_get_zfs_list_status=0
	zxfer_try_fast_recursive_noop_discovery || l_get_zfs_list_status=$?
	if [ "$l_get_zfs_list_status" -eq 1 ]; then
		# The no-op proof declined: run full discovery.
		l_get_zfs_list_status=0
		zxfer_run_full_snapshot_discovery || l_get_zfs_list_status=$?
	fi
	if [ "$l_get_zfs_list_status" -ne 0 ]; then
		zxfer_throw_error "${g_zxfer_snapshot_discovery_failure_result:-Failed to discover source and destination snapshots.}" \
			"$l_get_zfs_list_status"
		return "$l_get_zfs_list_status"
	fi
	# Without -R the only dataset to replicate is the initial source, whatever
	# the recursive listing found below it. This comes after the delta report
	# and the inventory decision, which describe the listing itself.
	[ -n "${g_option_R_recursive:-}" ] || g_recursive_source_list=$g_initial_source

	zxfer_echoV "End zxfer_get_zfs_list()"
}
