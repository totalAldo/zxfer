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
# owns globals: the source listing's command, PID and remote parallel
#   helper (g_source_snapshot_list_*, g_origin_parallel_cmd, _host), the fast
#   no-op proof's destination listing and optional pre-sorted listing handed to
#   full discovery (g_zxfer_snapshot_discovery_destination_listing_file,
#   g_zxfer_snapshot_discovery_destination_sorted_file), the destination
#   listing stderr scratch file, the staged snapshot record files
#   (g_zxfer_source_snapshot_record_cache_file,
#   g_zxfer_destination_snapshot_record_cache_file), the delta split counts and
#   file-pair provenance (g_zxfer_delta_missing_count,
#   g_zxfer_delta_extra_count, g_zxfer_delta_count_missing_file,
#   g_zxfer_delta_count_extra_file), the recursive work lists
#   (g_recursive_source_list, g_recursive_source_dataset_list,
#   g_recursive_destination_extra_dataset_list), and the in-shell results
#   g_zxfer_parallel_source_job_check_result,
#   g_zxfer_snapshot_discovery_failure_result, and
#   g_zxfer_recursive_dataset_list_result; also ZXFER_SNAPSHOT_RECORD_AWK and
#   ZXFER_SOURCE_DISCOVERY_SENTINEL.
# reads globals: discovery options (-R, -j, -x, -z, -O, -T, -P, -o, -U, -d,
#   -v, -V), g_initial_source, g_destination, the source and destination zfs,
#   awk, parallel and compression commands, and profiling state.
# mutates caches: the destination existence cache and dataset inventory,
#   through the destination-state helpers.
# returns via stdout: reversed record files and the -v/-V delta report;
#   launches registered background producers.
# Source command builders publish quoting's g_zxfer_shell_command_result;
# launchers retain it before producer preparation can render another command.
#
# zxfer_get_zfs_list first tries the fast recursive no-op proof, then runs
# full discovery. The fast proof has one owner for allocation, producer waits,
# validation, publication and release; only its raw-listing handoff (and,
# without -x when normalization succeeded, its pre-sorted destination listing)
# survives a fallback. Full discovery starts the creation-ordered source listing while
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

# Purpose: Reset all snapshot discovery state for a new zxfer session.
# Usage: zxfer_reset_snapshot_discovery_session_state; called once by the
# composition root before any discovery pass.
zxfer_reset_snapshot_discovery_session_state() {
	zxfer_reset_snapshot_discovery_state
	g_origin_parallel_cmd=""
	g_origin_parallel_cmd_host=""
	g_zxfer_destination_listing_error_file=""
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
# g_zxfer_shell_command_result. Retains PIPELINE before clearing the result;
# a failed SSH render publishes no partial command and preserves its status.
zxfer_render_origin_listing_command() {
	l_origin_listing=$1
	g_zxfer_shell_command_result=""
	l_origin_listing_compressed=0
	if [ "${g_option_z_compress:-0}" -eq 1 ]; then
		if [ -z "${g_origin_cmd_compress_safe:-}" ]; then
			g_zxfer_shell_command_result="The origin host compression command is not resolved."
			return 1
		fi
		l_origin_listing_compressed=1
		l_origin_listing="$l_origin_listing | $g_origin_cmd_compress_safe"
	fi
	zxfer_build_remote_sh_c_command "$l_origin_listing"
	zxfer_ssh_shell_command_for_host render "$g_option_O_origin_host" \
		"$g_zxfer_shell_command_result" || {
		set -- "$?"
		g_zxfer_shell_command_result=""
		return "$1"
	}
	l_origin_listing=$g_zxfer_shell_command_result
	[ "$l_origin_listing_compressed" -eq 0 ] ||
		l_origin_listing="$l_origin_listing | $g_cmd_decompress_safe"
	if [ "$2" -eq 1 ]; then
		zxfer_render_discovery_sentinel_filter_cmd
		l_origin_listing="$l_origin_listing | $g_zxfer_shell_command_result"
	fi
	g_zxfer_shell_command_result=$l_origin_listing
}

# Purpose: Build the source listing for the recursive no-op proof: one
# recursive name,guid stream of every source snapshot.
# Usage: zxfer_build_source_snapshot_name_list_cmd; publishes the shell
# command in g_zxfer_shell_command_result, or returns non-zero with
# an operator message (or nothing) there. The proof ignores -j, so it never
# pays parallel and per-dataset zfs startup before it knows there is work.
# GUIDs keep an exact-name divergence from passing as a no-op.
zxfer_build_source_snapshot_name_list_cmd() {
	g_zxfer_shell_command_result=""

	if [ -z "$g_option_O_origin_host" ]; then
		zxfer_render_shell_command_from_argv "$g_cmd_zfs" \
			list -Hr -o name,guid -t snapshot "$g_initial_source"
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

# Purpose: Render and start either source discovery pipeline, tracking every
# stage before the shared byte-sort, status check and cleanup registration.
# Usage: zxfer_start_source_snapshot_producer fast|full CMD SORTED [ERR [RAW]].
# Full discovery tees creation-ordered records to RAW. The fast proof filters
# exclusions before sorting. Publishes the registered PID in g_last_background_pid.
zxfer_start_source_snapshot_producer() {
	l_producer_mode=$1
	l_producer_sorted=$3
	l_producer_error=${4:-}
	if [ "$l_producer_mode" = full ]; then
		zxfer_create_runtime_artifact_workspace discovery-producer tee-status || return "$?"
		l_producer_workspace=$g_zxfer_runtime_artifact_path_result
		l_producer_purpose="background source snapshot discovery helper"
	else
		zxfer_get_temp_file || return "$?"
		l_producer_purpose="background source snapshot no-op proof helper"
	fi
	zxfer_escape_single_quotes_into_result "$g_zxfer_runtime_artifact_path_result"
	l_producer_quoted_path="'$g_zxfer_escaped_single_quotes_result'"
	l_producer_status_files=$l_producer_quoted_path
	l_producer_command="{ ( $2 ); printf '%s\n' \"\$?\" > $l_producer_quoted_path; }"
	if [ "$l_producer_mode" = full ]; then
		zxfer_escape_single_quotes_into_result "$l_producer_workspace.tee-status"
		l_producer_stage_status="'$g_zxfer_escaped_single_quotes_result'"
		l_producer_status_files="$l_producer_status_files $l_producer_stage_status"
		zxfer_escape_single_quotes_into_result "$5"
		l_producer_command="$l_producer_command | { tee '$g_zxfer_escaped_single_quotes_result'; printf '%s\n' \"\$?\" > $l_producer_stage_status; }"
	elif [ -n "${g_option_x_exclude_datasets:-}" ]; then
		zxfer_get_temp_file || return "$?"
		zxfer_escape_single_quotes_into_result "$g_zxfer_runtime_artifact_path_result"
		l_producer_stage_status="'$g_zxfer_escaped_single_quotes_result'"
		l_producer_status_files="$l_producer_status_files $l_producer_stage_status"
		zxfer_render_shell_command_from_argv "${g_cmd_awk:-awk}" \
			-v "exclude_pattern=$g_option_x_exclude_datasets" "$ZXFER_SNAPSHOT_RECORD_AWK"
		l_producer_command="$l_producer_command | { $g_zxfer_shell_command_result; printf '%s\n' \"\$?\" > $l_producer_stage_status; }"
	fi
	zxfer_escape_single_quotes_into_result "$l_producer_sorted"
	l_producer_status_check="l_sort_status=\$?
l_failed_status=0
for l_status_file in $l_producer_status_files; do
	l_status=1
	if [ -f \"\$l_status_file\" ]; then
		IFS= read -r l_status <\"\$l_status_file\" || l_status=1
	fi
	case \$l_status in
	'' | *[!0123456789]*) l_status=1 ;;
	esac
	[ \"\$l_failed_status\" -ne 0 ] || l_failed_status=\$l_status
done
rm -f $l_producer_status_files
[ \"\$l_failed_status\" -eq 0 ] || exit \"\$l_failed_status\"
exit \"\$l_sort_status\""
	l_producer_command="$l_producer_command | LC_ALL=C sort > '$g_zxfer_escaped_single_quotes_result'
$l_producer_status_check"

	zxfer_echoV "Executing command in the background: $l_producer_command"
	zxfer_record_last_command_string "$l_producer_command"
	zxfer_spawn_background_shell "$l_producer_command" /dev/null "$l_producer_error" || return "$?"
	zxfer_register_background_producer "$l_producer_purpose"
}

# Purpose: Build the creation-ordered source snapshot listing.
# Usage: zxfer_build_source_snapshot_list_cmd; publishes the shell command in
# g_zxfer_shell_command_result, or returns non-zero with an
# operator message (or nothing) there. With -j the listing fans out over the
# source datasets through parallel: the dataset list is captured first (exit
# 70 on failure), and only a successful parallel run adds the success sentinel
# that a local filter checks and strips.
zxfer_build_source_snapshot_list_cmd() {
	g_zxfer_shell_command_result=""

	if [ "$g_option_j_jobs" -le 1 ]; then
		zxfer_render_zfs_command_for_role source \
			list -Hr -o name,guid -s creation -t snapshot "$g_initial_source" || {
			set -- "$?"
			g_zxfer_shell_command_result=""
			return "$1"
		}
		return 0
	fi

	zxfer_ensure_parallel_available_for_source_jobs || {
		l_list_status=$?
		g_zxfer_shell_command_result=${g_zxfer_parallel_source_job_check_result:-Failed to prepare parallel source discovery.}
		return "$l_list_status"
	}
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
	g_zxfer_shell_command_result="$l_list_pipeline | $g_zxfer_shell_command_result"
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
		zxfer_throw_error "${g_zxfer_shell_command_result:-Failed to build source snapshot discovery command.}" "$?"
	l_source_snapshot_command=$g_zxfer_shell_command_result
	g_source_snapshot_list_cmd=$l_source_snapshot_command
	if [ "$g_option_O_origin_host" != "" ]; then
		g_zxfer_profile_source_ssh_shell_invocations=$((g_zxfer_profile_source_ssh_shell_invocations + 1))
	fi

	# A successful full listing uses parallel exactly when -j exceeds one.
	if [ "$g_option_j_jobs" -gt 1 ]; then
		g_zxfer_profile_source_snapshot_list_parallel_commands=$((g_zxfer_profile_source_snapshot_list_parallel_commands + 1))
	fi
	zxfer_echoV "Running command in the background: $l_source_snapshot_command"
	zxfer_record_last_command_string "$l_source_snapshot_command"
	zxfer_start_source_snapshot_producer full \
		"$l_source_snapshot_command" "$3" "$l_errfile" "$l_outfile" || return "$?"
	g_source_snapshot_list_pid=$g_last_background_pid
}

# Purpose: Rewrite a destination snapshot listing to source paths, optionally
# drop -x matches, and byte-sort the result in place.
# Usage: zxfer_normalize_destination_snapshot_list DEST_DATASET INPUT OUTPUT
# [EXCLUDE_PATTERN [quiet]]; OUTPUT gets the byte-sorted records that comm or
# cmp compares with the source listing.
zxfer_normalize_destination_snapshot_list() {
	if [ "${5:-}" != quiet ]; then
		if zxfer_command_trace_enabled; then
			zxfer_trace_rendered_command "Running command" \
				"$(zxfer_render_command_for_report "" "${g_cmd_awk:-awk}" \
					-v "destination_dataset=$1" -v "initial_source=$g_initial_source" \
					-v "exclude_pattern=${4:-}" \
					"$ZXFER_SNAPSHOT_RECORD_AWK" "$2") > $(zxfer_quote_token_for_report "$3") && $(zxfer_render_command_for_report "LC_ALL=C" sort -o "$3" "$3")"
		else
			zxfer_record_last_command_opaque
		fi
	fi
	"${g_cmd_awk:-awk}" -v "destination_dataset=$1" -v "initial_source=$g_initial_source" \
		-v "exclude_pattern=${4:-}" "$ZXFER_SNAPSHOT_RECORD_AWK" "$2" >"$3" || return "$?"
	# POSIX lets sort -o name one of its own input files.
	LC_ALL=C sort -o "$3" "$3"
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
		zxfer_read_runtime_artifact_file \
			"$l_destination_inventory_tmp_file" ||
			zxfer_throw_error "Failed to read staged destination dataset inventory." "$?"
		[ -n "$g_zxfer_runtime_artifact_read_result" ] || {
			zxfer_throw_error "Staged destination dataset inventory was empty."
		}
		zxfer_seed_destination_existence_cache_from_recursive_list "$g_destination" \
			"$g_zxfer_runtime_artifact_read_result"
		return
	fi

	zxfer_read_runtime_artifact_file \
		"$l_destination_inventory_err_file" ||
		zxfer_throw_error "Failed to read staged destination dataset inventory stderr." "$?"
	l_destination_inventory_error=$g_zxfer_runtime_artifact_read_result
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
			l_destination_inventory_pool_suffix=${l_destination_inventory_pool_error:+: $l_destination_inventory_pool_error}
			zxfer_throw_error "Destination dataset [$g_destination] is missing and destination pool [$l_destination_inventory_pool] could not be listed${l_destination_inventory_pool_suffix:-.}" "$l_destination_inventory_pool_status"
		fi
	else
		l_destination_inventory_error=$(zxfer_limit_snapshot_discovery_capture_lines \
			"$l_destination_inventory_error" 5)
		zxfer_throw_error "Failed to retrieve list of datasets from the destination${l_destination_inventory_error:+: $l_destination_inventory_error}" "$l_destination_inventory_status"
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
	zxfer_ensure_runtime_artifact_file "${g_zxfer_destination_listing_error_file:-}" \
		zxfer-destination-listing-err ||
		zxfer_throw_error "Failed to allocate the destination snapshot listing error file." "$?"
	g_zxfer_destination_listing_error_file=$g_zxfer_runtime_artifact_path_result
	zxfer_trace_zfs_command_for_role "Running command" destination \
		list -Hr -o name,guid -t snapshot "$l_dest_list_dataset"
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
			zxfer_destination_exists "$l_dest_list_dataset" live ||
				l_dest_list_exists=0
		fi
		if [ "$l_dest_list_exists" -ne 0 ]; then
			if zxfer_read_runtime_artifact_file "$g_zxfer_destination_listing_error_file" &&
				[ -n "$g_zxfer_runtime_artifact_read_result" ]; then
				zxfer_warn_stderr "$g_zxfer_runtime_artifact_read_result"
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
# Usage: zxfer_start_fast_noop_destination_producer OUTPUT ERR_FILE STATUS_FILE
# RAW_FILE; lists the destination root into RAW_FILE (kept for full discovery if
# the proof declines), writes its listing exit status to STATUS_FILE, then
# normalizes, filters -x, and byte-sorts into OUTPUT (exiting with that stage's
# status).
# Side effects: Publishes the registered producer PID in g_last_background_pid.
zxfer_start_fast_noop_destination_producer() {
	l_destination_sorted=$1
	l_dest_snapshot_err_file=$2
	l_dest_snapshot_status_file=$3
	l_dest_snapshot_raw_file=$4

	zxfer_map_destination_dataset
	l_destination_dataset=$g_zxfer_destination_dataset_result
	zxfer_trace_zfs_command_for_role "Running command in the background" \
		destination list -Hr -o name,guid -t snapshot "$l_destination_dataset"

	(
		l_list_status=0
		zxfer_run_destination_zfs_cmd list -Hr -o name,guid -t snapshot "$l_destination_dataset" \
			>"$l_dest_snapshot_raw_file" 2>"$l_dest_snapshot_err_file" || l_list_status=$?
		printf '%s\n' "$l_list_status" >"$l_dest_snapshot_status_file" 2>/dev/null || exit 1
		zxfer_normalize_destination_snapshot_list "$l_destination_dataset" \
			"$l_dest_snapshot_raw_file" "$l_destination_sorted" \
			"${g_option_x_exclude_datasets:-}" quiet
		exit "$?"
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
	zxfer_cleanup_runtime_artifact_path_list "${g_zxfer_source_snapshot_record_cache_file:-}
${g_zxfer_destination_snapshot_record_cache_file:-}"

	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
}

# Purpose: Reset the snapshot discovery state so the next pass starts clean.
# Usage: zxfer_reset_snapshot_discovery_state; called at the start of each
# discovery pass and by session and dry-run resets. The validated
# remote-parallel cache survives it and is cleared only by
# zxfer_reset_snapshot_discovery_session_state. The destination existence
# cache and inventory have their own reset in the destination-state module.
zxfer_reset_snapshot_discovery_state() {
	zxfer_cleanup_snapshot_record_cache_files
	g_source_snapshot_list_cmd=""
	g_source_snapshot_list_pid=""
	g_zxfer_parallel_source_job_check_result=""
	g_zxfer_snapshot_discovery_failure_result=""
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_recursive_dataset_list_result=""
	g_zxfer_snapshot_discovery_destination_listing_file=""
	g_zxfer_snapshot_discovery_destination_sorted_file=""
	g_zxfer_delta_missing_count=""
	g_zxfer_delta_extra_count=""
	g_zxfer_delta_count_missing_file=""
	g_zxfer_delta_count_extra_file=""
}

# Purpose: List the destination's datasets, on the -T host when one is given,
# and publish them as the recursive destination inventory.
# Usage: zxfer_collect_destination_dataset_inventory; called after the
# snapshot diff when later work reads the destination existence cache.
zxfer_collect_destination_dataset_inventory() {
	zxfer_create_runtime_artifact_workspace discovery-inventory error || return "$?"
	l_destination_inventory_workspace=$g_zxfer_runtime_artifact_path_result
	l_destination_inventory_stage_files=$g_zxfer_temp_file_group_result

	zxfer_trace_zfs_command_for_role "Running command" destination \
		list -t filesystem,volume -Hr -o name "$g_destination"
	l_dest_inventory_status=0
	zxfer_run_destination_zfs_cmd list -t filesystem,volume -Hr -o name "$g_destination" \
		>"$l_destination_inventory_workspace" 2>"$l_destination_inventory_workspace.error" ||
		l_dest_inventory_status=$?

	zxfer_publish_destination_dataset_inventory_from_stage \
		"$l_destination_inventory_workspace" "$l_destination_inventory_workspace.error" \
		"$l_dest_inventory_status" && l_dest_inventory_status=0 || l_dest_inventory_status=$?
	zxfer_cleanup_runtime_artifact_path_list "$l_destination_inventory_stage_files"
	return "$l_dest_inventory_status"
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
	l_reverse_numbered_file=$g_zxfer_runtime_artifact_path_result
	l_reverse_status=0
	# grep -n prefixes every line, including empty ones, with "number:".
	# Finish the sort before emitting rows so a failed sort cannot look empty.
	LC_ALL=C grep -n '^' "$1" >"$l_reverse_numbered_file" &&
		LC_ALL=C sort -nr -o "$l_reverse_numbered_file" "$l_reverse_numbered_file" &&
		LC_ALL=C cut -d : -f2- "$l_reverse_numbered_file" || l_reverse_status=$?
	zxfer_cleanup_runtime_artifact_path "$l_reverse_numbered_file"
	return "$l_reverse_status"
}

# Purpose: Derive all snapshot dataset lists in one extraction and keyed sort.
# Usage: zxfer_capture_snapshot_dataset_lists MISSING EXTRA SCRATCH [SOURCE];
# publishes the transfer, extra and optional source-inventory lists. Tags keep
# each list sorted and deduplicated independently. Values stay in files/stdin.
# Returns: The original awk/sort failure with every output empty.
zxfer_capture_snapshot_dataset_lists() {
	g_recursive_source_list=""
	g_recursive_destination_extra_dataset_list=""
	g_recursive_source_dataset_list=""
	[ -s "$1" ] || [ -s "$2" ] || [ -n "${4:-}" ] || return 0
	l_dataset_lists_scratch=$3
	l_dataset_lists_source=${4:-}
	set -- role=missing "$1" role=extra "$2"
	[ -z "$l_dataset_lists_source" ] || set -- "$@" role=inventory "$l_dataset_lists_source"
	# Each sorted input groups snapshots by dataset. Names still need a sort:
	# a child's full snapshot record sorts before its parent's (@ follows /).
	# shellcheck disable=SC2016 # awk reads literal fields; role is a fixed tag.
	"${g_cmd_awk:-awk}" -F@ '
		$1 != "" && (role != previous_role || $1 != previous_dataset) {
			print role "\t" $1
			previous_role = role
			previous_dataset = $1
		}
	' "$@" >"$l_dataset_lists_scratch" || return "$?"
	l_dataset_lists_rows=$(LC_ALL=C sort -u "$l_dataset_lists_scratch") || return "$?"
	while IFS="$ZXFER_TAB" read -r l_dataset_lists_kind l_dataset_lists_name; do
		case $l_dataset_lists_kind in
		missing) g_recursive_source_list=${g_recursive_source_list:+$g_recursive_source_list$ZXFER_LF}$l_dataset_lists_name ;;
		extra) g_recursive_destination_extra_dataset_list=${g_recursive_destination_extra_dataset_list:+$g_recursive_destination_extra_dataset_list$ZXFER_LF}$l_dataset_lists_name ;;
		inventory) g_recursive_source_dataset_list=${g_recursive_source_dataset_list:+$g_recursive_source_dataset_list$ZXFER_LF}$l_dataset_lists_name ;;
		esac
	done <<EOF
$l_dataset_lists_rows
EOF
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

# Purpose: Decide whether recursive source dataset inventory must be derived
# from the full source snapshot list.
# Usage: zxfer_snapshot_discovery_needs_source_dataset_inventory [SOURCE_WORK];
# SOURCE_WORK previews whether a missing-record file has work before extraction.
# Without it, reads the published transfer list. Returns 1
# for a recursive run with no property work (-P, -o, or -U with source
# datasets to scan), which never reads the inventory.
zxfer_snapshot_discovery_needs_source_dataset_inventory() {
	[ -z "${g_option_R_recursive:-}" ] ||
		zxfer_property_pass_is_enabled ||
		{ [ "${g_option_U_skip_unsupported_properties:-0}" -eq 1 ] &&
			[ -n "${1-${g_recursive_source_list:-}}" ]; }
}

# Purpose: Split two sorted record files into the records only the source has
# and the records only the destination has.
# Usage: zxfer_write_snapshot_delta_files SOURCE DESTINATION MISSING EXTRA
# SCRATCH; truncates and writes MISSING and EXTRA in sorted order and
# overwrites SCRATCH.
# Side effects: Publishes g_zxfer_delta_missing_count,
# g_zxfer_delta_extra_count, and file-pair provenance
# (g_zxfer_delta_count_missing_file, g_zxfer_delta_count_extra_file) after both
# outputs close cleanly.
zxfer_write_snapshot_delta_files() {
	g_zxfer_delta_missing_count=""
	g_zxfer_delta_extra_count=""
	g_zxfer_delta_count_missing_file=""
	g_zxfer_delta_count_extra_file=""

	LC_ALL=C comm -3 "$1" "$2" >"$5" || return "$?"
	# comm -3 indents destination-only records with one tab. A snapshot name
	# cannot start with a tab, so dropping exactly one keeps the record.
	# Both outputs are opened via ENVIRON so stdout can return their counts.
	# shellcheck disable=SC2016  # awk program should see literal $0 and ENVIRON.
	l_delta_split_counts=$(
		ZXFER_AWK_DELTA_MISSING_FILE=$3
		ZXFER_AWK_DELTA_EXTRA_FILE=$4
		export ZXFER_AWK_DELTA_MISSING_FILE ZXFER_AWK_DELTA_EXTRA_FILE
		"${g_cmd_awk:-awk}" '
			BEGIN {
				missing_file = ENVIRON["ZXFER_AWK_DELTA_MISSING_FILE"]
				extra_file = ENVIRON["ZXFER_AWK_DELTA_EXTRA_FILE"]
				printf "" > missing_file
				printf "" > extra_file
			}
			substr($0, 1, 1) == "\t" {
				print substr($0, 2) > extra_file
				extra++
				next
			}
			{
				print > missing_file
				missing++
			}
			END {
				close_failed = 0
				if (close(missing_file) != 0) close_failed = 1
				if (close(extra_file) != 0) close_failed = 1
				if (close_failed) exit 1
				print missing + 0, extra + 0
			}' "$5"
	) || return "$?"
	l_delta_split_missing=${l_delta_split_counts%% *}
	l_delta_split_extra=${l_delta_split_counts#* }
	if zxfer_is_uint "$l_delta_split_missing" &&
		zxfer_is_uint "$l_delta_split_extra" &&
		[ "$l_delta_split_counts" = "$l_delta_split_missing $l_delta_split_extra" ]; then
		g_zxfer_delta_missing_count=$l_delta_split_missing
		g_zxfer_delta_extra_count=$l_delta_split_extra
		g_zxfer_delta_count_missing_file=$3
		g_zxfer_delta_count_extra_file=$4
	fi
}

# Purpose: Print the recursive delta summary and snapshot identities under
# -v, and the raw normalized delta records as well under -V.
# Usage: zxfer_report_recursive_snapshot_delta MISSING EXTRA; call once the
# recursive dataset lists are final. Reuses provenanced split counts from
# zxfer_write_snapshot_delta_files, fast-paths empty readable files to 0/0,
# counts final dataset lists in-shell, and falls back to awk for standalone
# callers without matching provenance.
zxfer_report_recursive_snapshot_delta() {
	l_report_delta_missing_file=$1
	l_report_delta_extra_file=$2

	if [ "${g_option_v_verbose:-0}" -ne 1 ] && [ "${g_option_V_very_verbose:-0}" -ne 1 ]; then
		return 0
	fi

	if [ -r "$l_report_delta_missing_file" ] && [ ! -s "$l_report_delta_missing_file" ] &&
		[ -r "$l_report_delta_extra_file" ] && [ ! -s "$l_report_delta_extra_file" ]; then
		l_report_delta_missing_count=0
		l_report_delta_extra_count=0
	elif [ -n "${g_zxfer_delta_missing_count:-}" ] &&
		[ -n "${g_zxfer_delta_extra_count:-}" ] &&
		[ "${g_zxfer_delta_count_missing_file:-}" = "$l_report_delta_missing_file" ] &&
		[ "${g_zxfer_delta_count_extra_file:-}" = "$l_report_delta_extra_file" ]; then
		l_report_delta_missing_count=$g_zxfer_delta_missing_count
		l_report_delta_extra_count=$g_zxfer_delta_extra_count
		g_zxfer_delta_missing_count=""
		g_zxfer_delta_extra_count=""
		g_zxfer_delta_count_missing_file=""
		g_zxfer_delta_count_extra_file=""
	else
		# Standalone callers without provenance count the delta files directly.
		# shellcheck disable=SC2016  # awk program should see literal ARGV/FILENAME.
		l_report_delta_counts=$(
			"${g_cmd_awk:-awk}" '
			FILENAME == ARGV[1] { missing++; next }
			FILENAME == ARGV[2] { extra++; next }
			END { print missing + 0, extra + 0 }' \
				"$l_report_delta_missing_file" "$l_report_delta_extra_file"
		) || zxfer_throw_error "Failed to count the recursive snapshot delta." "$?"
		read -r l_report_delta_missing_count l_report_delta_extra_count l_report_delta_unused <<-EOF
			$l_report_delta_counts
		EOF
	fi

	# Count the final post-filter/-prune dataset lists in the shell (non-blank
	# lines matching awk NF, avoiding argv/env size limits and extra forks).
	l_report_delta_source_dataset_count=0
	if [ -n "$g_recursive_source_list" ]; then
		while IFS=" $ZXFER_TAB" read -r l_report_delta_token l_report_delta_rest; do
			[ -z "$l_report_delta_token" ] ||
				l_report_delta_source_dataset_count=$((l_report_delta_source_dataset_count + 1))
		done <<EOF
$g_recursive_source_list
EOF
	fi
	l_report_delta_extra_dataset_count=0
	if [ -n "$g_recursive_destination_extra_dataset_list" ]; then
		while IFS=" $ZXFER_TAB" read -r l_report_delta_token l_report_delta_rest; do
			[ -z "$l_report_delta_token" ] ||
				l_report_delta_extra_dataset_count=$((l_report_delta_extra_dataset_count + 1))
		done <<EOF
$g_recursive_destination_extra_dataset_list
EOF
	fi

	if [ "${l_report_delta_missing_count:-0}" -gt 0 ] || [ "${l_report_delta_extra_count:-0}" -gt 0 ]; then
		zxfer_echov "Recursive snapshot delta summary: snapshots_missing_on_destination=$l_report_delta_missing_count snapshots_missing_on_source=$l_report_delta_extra_count source_datasets=$l_report_delta_source_dataset_count destination_extra_datasets=$l_report_delta_extra_dataset_count"
		if [ -n "$g_recursive_source_list" ]; then
			zxfer_echov "Recursive source datasets queued for transfer:"
			while IFS= read -r l_report_delta_source_dataset; do
				[ -n "$l_report_delta_source_dataset" ] || continue
				zxfer_echov "  $l_report_delta_source_dataset"
			done <<EOF
$g_recursive_source_list
EOF
		fi
		if [ -n "$g_recursive_destination_extra_dataset_list" ]; then
			zxfer_echov "Recursive destination datasets queued for delete inspection:"
			while IFS= read -r l_report_delta_destination_dataset; do
				[ -n "$l_report_delta_destination_dataset" ] || continue
				zxfer_echov "  $l_report_delta_destination_dataset"
			done <<EOF
$g_recursive_destination_extra_dataset_list
EOF
		fi
		# Compare full name/GUID identities: a name collision with a different
		# GUID appears on both sides. Destination delta rows use source paths;
		# map them back for the operator without another ZFS listing. Quote
		# "source": after `EOF` + `fi`, ksh93's lexer can alias-expand a bare
		# word after `for ... in` via its built-in `source='command .'` alias.
		for l_report_delta_side in "source" "destination"; do
			case $l_report_delta_side in
			source)
				l_report_delta_records_file=$l_report_delta_missing_file
				l_report_delta_record_count=$l_report_delta_missing_count
				l_report_delta_other_side=destination
				;;
			destination)
				l_report_delta_records_file=$l_report_delta_extra_file
				l_report_delta_record_count=$l_report_delta_extra_count
				l_report_delta_other_side=source
				;;
			esac
			[ "$l_report_delta_record_count" -gt 0 ] || continue
			l_report_delta_read_error="Failed to read the recursive $l_report_delta_side snapshot delta."
			[ -r "$l_report_delta_records_file" ] || zxfer_throw_error "$l_report_delta_read_error"
			zxfer_echov "Recursive $l_report_delta_side snapshots absent from $l_report_delta_other_side (name/GUID):"
			while IFS="$ZXFER_TAB" read -r l_report_delta_snapshot l_report_delta_guid; do
				if [ "$l_report_delta_side" = destination ]; then
					zxfer_map_destination_dataset "${l_report_delta_snapshot%@*}"
					l_report_delta_snapshot=$g_zxfer_destination_dataset_result@${l_report_delta_snapshot#*@}
				fi
				zxfer_echov "  $l_report_delta_snapshot (GUID: ${l_report_delta_guid:-missing})"
				if [ "$l_report_delta_side" = source ] && [ -z "${g_option_R_recursive:-}" ] &&
					[ "${l_report_delta_snapshot%@*}" != "$g_initial_source" ]; then
					# -d also queues destination-extra datasets, even under -N.
					# Do not label a descendant skipped when cleanup will visit it.
					l_report_delta_selected=0
					if [ "${g_option_d_delete_destination_snapshots:-0}" -eq 1 ]; then
						case "$ZXFER_LF$g_recursive_destination_extra_dataset_list$ZXFER_LF" in
						*"$ZXFER_LF${l_report_delta_snapshot%@*}$ZXFER_LF"*) l_report_delta_selected=1 ;;
						esac
					fi
					[ "$l_report_delta_selected" -eq 1 ] ||
						zxfer_echov "    Not scheduled: nonrecursive mode (-N) selects only [$g_initial_source]; this descendant is outside the dataset queue."
				fi
			done <"$l_report_delta_records_file" ||
				zxfer_throw_error "$l_report_delta_read_error" "$?"
		done
	fi

	[ "${g_option_V_very_verbose:-0}" -eq 1 ] || return 0
	echo "====================================================================="
	echo "====== Snapshots present in source but missing in destination ======"
	[ -s "$l_report_delta_missing_file" ] && cat "$l_report_delta_missing_file"
	echo "====== Source datasets that differ from destination ======"
	echo "g_recursive_source_list:"
	printf '%s\n' "$g_recursive_source_list"
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
# Usage: zxfer_set_g_recursive_source_list SORTED_SOURCE SORTED_DESTINATION
# Publishes g_recursive_source_list, g_recursive_destination_extra_dataset_list
# and, when later work needs it, g_recursive_source_dataset_list, all without
# -x matches. Recursive -d work excludes datasets absent from a successful
# live source dataset inventory, preserving target-only datasets and snapshots.
# Failures throw.
zxfer_set_g_recursive_source_list() {
	l_delta_source_file=$1
	l_delta_destination_file=$2
	[ -f "$l_delta_source_file" ] ||
		zxfer_throw_error "Failed to locate staged sorted source snapshots for recursive delta planning."

	# The primary file is scratch for the diff, dataset sort and live inventory.
	set -- missing extra
	[ -z "${g_option_x_exclude_datasets:-}" ] || set -- "$@" filtered-source filtered-destination
	zxfer_create_runtime_artifact_workspace discovery-delta "$@" || return "$?"
	l_delta_workspace=$g_zxfer_runtime_artifact_path_result
	l_delta_stage_files=$g_zxfer_temp_file_group_result

	# -x first drops excluded records, so differences confined to excluded
	# datasets cannot keep a run from comparing as a no-op.
	if [ -n "${g_option_x_exclude_datasets:-}" ]; then
		zxfer_filter_snapshot_file_with_excludes "$l_delta_source_file" "$l_delta_workspace.filtered-source" ||
			zxfer_throw_error "Failed to filter source snapshots against exclude patterns for recursive delta planning." "$?"
		zxfer_filter_snapshot_file_with_excludes "$l_delta_destination_file" "$l_delta_workspace.filtered-destination" ||
			zxfer_throw_error "Failed to filter destination snapshots against exclude patterns for recursive delta planning." "$?"
		l_delta_source_file=$l_delta_workspace.filtered-source
		l_delta_destination_file=$l_delta_workspace.filtered-destination
	fi

	# Equal listings leave both freshly allocated delta files empty.
	g_zxfer_delta_missing_count=""
	g_zxfer_delta_extra_count=""
	g_zxfer_delta_count_missing_file=""
	g_zxfer_delta_count_extra_file=""
	l_delta_compare_status=0
	cmp -s "$l_delta_source_file" "$l_delta_destination_file" || l_delta_compare_status=$?
	case $l_delta_compare_status in
	0) ;;
	1)
		zxfer_write_snapshot_delta_files "$l_delta_source_file" "$l_delta_destination_file" \
			"$l_delta_workspace.missing" "$l_delta_workspace.extra" "$l_delta_workspace" ||
			zxfer_throw_error "Failed to diff source and destination snapshots for recursive delta planning." "$?"
		;;
	*)
		zxfer_throw_error "Failed to compare source and destination snapshots for recursive delta planning." "$l_delta_compare_status"
		;;
	esac

	l_delta_source_work=""
	[ ! -s "$l_delta_workspace.missing" ] || l_delta_source_work=1
	set -- "$l_delta_workspace.missing" "$l_delta_workspace.extra" "$l_delta_workspace"
	if zxfer_snapshot_discovery_needs_source_dataset_inventory "$l_delta_source_work"; then
		set -- "$@" "$l_delta_source_file"
	fi
	l_delta_dataset_label="recursive source dataset inventory"
	if [ -s "$l_delta_workspace.missing" ]; then
		l_delta_dataset_label="recursive source dataset transfer list"
	elif [ -s "$l_delta_workspace.extra" ]; then
		l_delta_dataset_label="recursive destination dataset delete list"
	fi
	zxfer_capture_snapshot_dataset_lists "$@" ||
		zxfer_throw_error "Failed to derive $l_delta_dataset_label." "$?"
	zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_source_list" ||
		zxfer_throw_error "Failed to filter recursive source dataset transfer list against exclude patterns." "$?"
	g_recursive_source_list=$g_zxfer_recursive_dataset_list_result
	zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_destination_extra_dataset_list" ||
		zxfer_throw_error "Failed to filter recursive destination dataset delete list against exclude patterns." "$?"
	g_recursive_destination_extra_dataset_list=$g_zxfer_recursive_dataset_list_result
	if [ -n "${g_option_R_recursive:-}" ] &&
		[ "$g_option_d_delete_destination_snapshots" -eq 1 ] &&
		[ -n "$g_recursive_destination_extra_dataset_list" ]; then
		# Snapshot rows cannot distinguish an absent dataset from an existing
		# dataset with no snapshots. List datasets before admitting -d work.
		zxfer_run_source_zfs_cmd list -Hr -t filesystem,volume -o name "$g_initial_source" \
			>"$l_delta_workspace" ||
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
			' side=source "$l_delta_workspace" side=extra - <<EOF
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
	if zxfer_snapshot_discovery_needs_source_dataset_inventory; then
		zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_source_dataset_list" ||
			zxfer_throw_error "Failed to filter recursive source dataset inventory against exclude patterns." "$?"
		g_recursive_source_dataset_list=$g_zxfer_recursive_dataset_list_result
	else
		g_recursive_source_dataset_list=""
	fi

	zxfer_report_recursive_snapshot_delta "$l_delta_workspace.missing" "$l_delta_workspace.extra"
	if [ "$g_recursive_source_list" = "" ]; then
		zxfer_echov "No new snapshots to transfer during discovery for [$g_initial_source]."
	fi
	zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
}

# Purpose: Decide whether this pass has transfer, deletion, or property work.
# Usage: zxfer_pass_has_replication_work, before collecting the destination
# inventory in discovery and before building the dataset queue in replication.
zxfer_pass_has_replication_work() {
	[ -n "${g_recursive_source_list:-}" ] ||
		{ [ "${g_option_d_delete_destination_snapshots:-0}" -eq 1 ] &&
			[ -n "${g_recursive_destination_extra_dataset_list:-}" ]; } ||
		zxfer_property_pass_is_enabled
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

# Purpose: Preserve a failed source producer's command, stderr and wait status.
# Usage: zxfer_validate_source_snapshot_wait_status STATUS ERROR_FILE, from
# fast or full discovery after wait. Zero needs no stderr read; a failed read
# takes precedence over the producer status. The operation owner reports the
# diagnostic in g_zxfer_snapshot_discovery_failure_result after releasing scratch.
zxfer_validate_source_snapshot_wait_status() {
	[ "$1" -ne 0 ] || return 0
	if [ -n "${g_source_snapshot_list_cmd:-}" ]; then
		zxfer_record_last_command_string "$g_source_snapshot_list_cmd"
	fi
	zxfer_read_runtime_artifact_file "$2" || {
		l_source_snapshot_failure_read_status=$?
		g_zxfer_snapshot_discovery_failure_result="Failed to read staged source snapshot stderr."
		return "$l_source_snapshot_failure_read_status"
	}
	l_source_snapshot_failure_error=$(zxfer_limit_snapshot_discovery_capture_lines \
		"$g_zxfer_runtime_artifact_read_result" 10)
	g_zxfer_snapshot_discovery_failure_result="Failed to retrieve snapshots from the source${l_source_snapshot_failure_error:+: $l_source_snapshot_failure_error}"
	return "$1"
}

# Purpose: Stop and reap a registered fast no-op source producer when
# destination setup fails.
# Usage: zxfer_abort_registered_fast_noop_producer PID PURPOSE; keeps
# registered cleanup abort so a registered producer never falls back to bare
# direct-child signalling.
zxfer_abort_registered_fast_noop_producer() {
	zxfer_abort_fast_noop_background_pid "$1" "$2" || return 0
	zxfer_cleanup_pid_abort_grace_wait
	zxfer_abort_cleanup_pid "$1" KILL || return 0
	wait "$1" 2>/dev/null || :
	zxfer_unregister_cleanup_pid "$1"
	g_last_background_pid=""
}

# Purpose: Validate equal fast no-op proof streams and publish a proven no-op
# or decide fallback versus failure.
# Usage: zxfer_validate_fast_recursive_noop_equal_result WORKSPACE STATUS_VALID
# LIST_STATUS DEST_STATUS SOURCE_STATUS; preserves the 6-step failure
# precedence without touching the owner's l_fast_noop_owner_* scratch.
# Returns: 0 when proven, 1 for full-discovery fallback, or a non-zero status
# with g_zxfer_snapshot_discovery_failure_result populated when the owner must
# throw.
zxfer_validate_fast_recursive_noop_equal_result() {
	l_noop_valid_workspace=$1
	l_noop_valid_status_valid=$2
	l_noop_valid_list_status=$3
	l_noop_valid_dest_status=$4
	l_noop_valid_source_status=$5

	if [ "$l_noop_valid_status_valid" -ne 1 ]; then
		g_zxfer_snapshot_discovery_failure_result="Failed to validate destination snapshot status for recursive no-op proof."
		return 1
	fi
	if [ "$l_noop_valid_list_status" -ne 0 ]; then
		l_noop_valid_read_status=0
		zxfer_read_runtime_artifact_file "$l_noop_valid_workspace.destination-error" ||
			l_noop_valid_read_status=$?
		if [ "$l_noop_valid_read_status" -ne 0 ]; then
			g_zxfer_snapshot_discovery_failure_result="Failed to read staged destination snapshot stderr."
			return "$l_noop_valid_read_status"
		fi
		if ! zxfer_destination_probe_reports_missing "$g_zxfer_runtime_artifact_read_result"; then
			[ -z "$g_zxfer_runtime_artifact_read_result" ] ||
				printf '%s\n' "$g_zxfer_runtime_artifact_read_result" >&2
			g_zxfer_snapshot_discovery_failure_result="Failed to retrieve snapshot list from the destination."
			return "$l_noop_valid_list_status"
		fi
	fi
	if [ "$l_noop_valid_dest_status" -ne 0 ]; then
		return "$l_noop_valid_dest_status"
	fi
	zxfer_validate_source_snapshot_wait_status \
		"$l_noop_valid_source_status" "$l_noop_valid_workspace.error" ||
		return "$?"
	if [ ! -s "$l_noop_valid_workspace" ]; then
		if [ -n "${g_option_x_exclude_datasets:-}" ]; then
			zxfer_reset_destination_existence_cache
		else
			g_zxfer_snapshot_discovery_failure_result="Failed to retrieve snapshots from the source"
		fi
		return 1
	fi
	if [ "$l_noop_valid_list_status" -ne 0 ]; then
		zxfer_map_destination_dataset
		zxfer_echoV "Destination dataset does not exist: $g_zxfer_destination_dataset_result"
		zxfer_reset_destination_existence_cache
		return 1
	fi
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_snapshot_discovery_destination_listing_file=""
	g_zxfer_snapshot_discovery_destination_sorted_file=""
	zxfer_echov "No new snapshots to transfer during discovery for [$g_initial_source]."
	return 0
}

# Purpose: Own a fast recursive no-op proof from allocation through publication
# or fallback, before the full creation-order source discovery path.
# Usage: zxfer_try_fast_recursive_noop_discovery; returns 0 when proven,
# 1 for full discovery, other statuses on failure. Only this function uses
# l_fast_noop_owner_* scratch; one named file workspace survives helper calls.
# Registered producers remain trap-owned. On fallback, retains RAW and (when
# destination normalization succeeded without -x) the pre-sorted listing.
zxfer_try_fast_recursive_noop_discovery() {
	zxfer_fast_recursive_noop_options_are_eligible || return 1
	g_zxfer_snapshot_discovery_failure_result=""
	g_zxfer_snapshot_discovery_destination_listing_file=""
	g_zxfer_snapshot_discovery_destination_sorted_file=""
	zxfer_create_runtime_artifact_workspace discovery-fast-source \
		destination error destination-error status raw || return "$?"
	l_fast_noop_workspace=$g_zxfer_runtime_artifact_path_result
	l_fast_noop_paths=$g_zxfer_temp_file_group_result
	l_fast_noop_owner_status=0

	zxfer_profile_start_timer
	l_fast_noop_owner_source_start_ms=$g_zxfer_profile_clock_ms
	if zxfer_build_source_snapshot_name_list_cmd; then
		l_fast_noop_source_cmd=$g_zxfer_shell_command_result
		g_source_snapshot_list_cmd=$l_fast_noop_source_cmd
		g_zxfer_profile_source_snapshot_list_commands=$((g_zxfer_profile_source_snapshot_list_commands + 1))
		[ -z "$g_option_O_origin_host" ] ||
			g_zxfer_profile_source_ssh_shell_invocations=$((g_zxfer_profile_source_ssh_shell_invocations + 1))
		zxfer_start_source_snapshot_producer fast "$l_fast_noop_source_cmd" \
			"$l_fast_noop_workspace" "$l_fast_noop_workspace.error" ||
			l_fast_noop_owner_status=$?
	else
		l_fast_noop_owner_status=$?
	fi

	if [ "$l_fast_noop_owner_status" -eq 0 ]; then
		l_fast_noop_owner_source_pid=$g_last_background_pid
		zxfer_profile_start_timer
		l_fast_noop_owner_destination_start_ms=$g_zxfer_profile_clock_ms
		zxfer_start_fast_noop_destination_producer \
			"$l_fast_noop_workspace.destination" "$l_fast_noop_workspace.destination-error" \
			"$l_fast_noop_workspace.status" "$l_fast_noop_workspace.raw" ||
			l_fast_noop_owner_status=$?
		[ "$l_fast_noop_owner_status" -eq 0 ] ||
			zxfer_abort_registered_fast_noop_producer "$l_fast_noop_owner_source_pid" \
				"background source snapshot no-op proof helper"
	fi

	if [ "$l_fast_noop_owner_status" -eq 0 ]; then
		l_fast_noop_owner_destination_pid=$g_last_background_pid
		l_fast_noop_owner_source_status=0
		zxfer_wait_for_snapshot_discovery_producer "$l_fast_noop_owner_source_pid" \
			"$l_fast_noop_owner_source_start_ms" source || l_fast_noop_owner_source_status=$?
		l_fast_noop_owner_destination_status=0
		zxfer_wait_for_snapshot_discovery_producer "$l_fast_noop_owner_destination_pid" \
			"$l_fast_noop_owner_destination_start_ms" destination || l_fast_noop_owner_destination_status=$?
		g_last_background_pid=""

		# Parse the destination listing status once before comparison so even a
		# declined proof can hand RAW (and, without -x, the pre-sorted listing)
		# over to full discovery.
		l_fast_noop_owner_status_valid=0
		l_fast_noop_owner_list_status=""
		if [ -f "$l_fast_noop_workspace.status" ]; then
			read -r l_fast_noop_owner_list_status <"$l_fast_noop_workspace.status" || :
			! zxfer_is_uint "$l_fast_noop_owner_list_status" || l_fast_noop_owner_status_valid=1
		fi
		if [ "$l_fast_noop_owner_status_valid" -eq 1 ] && [ "$l_fast_noop_owner_list_status" -eq 0 ]; then
			g_zxfer_snapshot_discovery_destination_listing_file=$l_fast_noop_workspace.raw
			[ "$l_fast_noop_owner_destination_status" -ne 0 ] ||
				[ -n "${g_option_x_exclude_datasets:-}" ] ||
				[ ! -f "$l_fast_noop_workspace.destination" ] ||
				g_zxfer_snapshot_discovery_destination_sorted_file=$l_fast_noop_workspace.destination
		fi

		zxfer_profile_start_timer
		l_fast_noop_compare_start_ms=$g_zxfer_profile_clock_ms
		cmp -s "$l_fast_noop_workspace" "$l_fast_noop_workspace.destination" ||
			l_fast_noop_owner_status=$?
		zxfer_profile_stop_timer "$l_fast_noop_compare_start_ms"
		g_zxfer_profile_snapshot_diff_sort_ms=$((g_zxfer_profile_snapshot_diff_sort_ms + g_zxfer_profile_elapsed_ms))

		case $l_fast_noop_owner_status in
		0)
			zxfer_validate_fast_recursive_noop_equal_result \
				"$l_fast_noop_workspace" "$l_fast_noop_owner_status_valid" \
				"$l_fast_noop_owner_list_status" "$l_fast_noop_owner_destination_status" \
				"$l_fast_noop_owner_source_status" || l_fast_noop_owner_status=$?
			;;
		1) zxfer_reset_destination_existence_cache ;;
		*) g_zxfer_snapshot_discovery_failure_result="Failed to compare source and destination snapshots for recursive no-op proof." ;;
		esac
	fi

	if [ "$l_fast_noop_owner_status" -ne 0 ]; then
		# Success ends the pass: EXIT removes its private root. On fallback or
		# failure, release all scratch except the validated handoff file(s).
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_noop_paths" \
			"$g_zxfer_snapshot_discovery_destination_listing_file" \
			"$g_zxfer_snapshot_discovery_destination_sorted_file"
	fi
	[ -z "$g_zxfer_snapshot_discovery_failure_result" ] ||
		zxfer_throw_error "$g_zxfer_snapshot_discovery_failure_result" "$l_fast_noop_owner_status"
	return "$l_fast_noop_owner_status"
}

# Purpose: Own full discovery's allocation, overlapping producers, validation,
# publication and release.
# Usage: zxfer_run_full_snapshot_discovery; one named workspace retains the
# source, error, sorted, mapped and raw paths across calls. A declined proof
# may supply the raw destination listing (and, without -x, its pre-sorted
# normalized listing) instead. Timers/statuses use l_full_owner_*.
# Only validated record caches and the registered source PID survive this
# operation. An unreaped producer keeps its files for the immediate failure
# throw's EXIT trap, which stops producers before removing the run root.
zxfer_run_full_snapshot_discovery() {
	l_full_reused_raw=${g_zxfer_snapshot_discovery_destination_listing_file:-}
	l_full_reused_sorted=${g_zxfer_snapshot_discovery_destination_sorted_file:-}
	g_zxfer_snapshot_discovery_destination_listing_file=""
	g_zxfer_snapshot_discovery_destination_sorted_file=""
	l_full_owner_reuse_sorted=0
	if [ -n "$l_full_reused_raw" ] &&
		[ -n "$l_full_reused_sorted" ] &&
		[ -f "$l_full_reused_sorted" ] &&
		[ -z "${g_option_x_exclude_datasets:-}" ]; then
		l_full_owner_reuse_sorted=1
	fi

	set -- error sorted
	[ "$l_full_owner_reuse_sorted" -eq 1 ] || set -- "$@" mapped
	[ -n "$l_full_reused_raw" ] || set -- "$@" raw
	zxfer_create_runtime_artifact_workspace discovery-source "$@" || {
		l_full_owner_status=$?
		zxfer_cleanup_runtime_artifact_path_list "$l_full_reused_raw$ZXFER_LF$l_full_reused_sorted"
		return "$l_full_owner_status"
	}
	l_full_workspace=$g_zxfer_runtime_artifact_path_result
	l_full_workspace_paths=$g_zxfer_temp_file_group_result
	[ -z "$l_full_reused_raw" ] ||
		l_full_workspace_paths=$l_full_workspace_paths$ZXFER_LF$l_full_reused_raw
	[ -z "$l_full_reused_sorted" ] ||
		l_full_workspace_paths=$l_full_workspace_paths$ZXFER_LF$l_full_reused_sorted
	l_full_workspace_destination=${l_full_reused_raw:-$l_full_workspace.raw}
	if [ "$l_full_owner_reuse_sorted" -eq 1 ]; then
		l_full_workspace_mapped=$l_full_reused_sorted
	else
		l_full_workspace_mapped=$l_full_workspace.mapped
	fi
	g_source_snapshot_list_pid=""
	zxfer_profile_start_timer
	l_full_owner_source_start_ms=$g_zxfer_profile_clock_ms
	l_full_owner_status=0
	zxfer_write_source_snapshot_list_to_file "$l_full_workspace" "$l_full_workspace.error" "$l_full_workspace.sorted" || l_full_owner_status=$?

	# The destination is collected while the source producer runs. A declined
	# fast proof can hand over its successful raw listing (and, without -x,
	# its already-normalized byte-sorted file) without another ZFS/awk/sort.
	if [ "$l_full_owner_status" -eq 0 ]; then
		zxfer_profile_start_timer
		l_full_owner_destination_start_ms=$g_zxfer_profile_clock_ms
		zxfer_map_destination_dataset
		l_full_owner_destination=$g_zxfer_destination_dataset_result
		if [ "$l_full_owner_reuse_sorted" -eq 1 ]; then
			zxfer_set_destination_existence_cache_entry "$l_full_owner_destination" 1
		elif [ -n "$l_full_reused_raw" ]; then
			zxfer_set_destination_existence_cache_entry "$l_full_owner_destination" 1
			zxfer_normalize_destination_snapshot_list "$l_full_owner_destination" \
				"$l_full_workspace_destination" "$l_full_workspace_mapped" || l_full_owner_status=$?
		else
			zxfer_write_destination_snapshot_list_to_files "$l_full_workspace_destination" "$l_full_workspace_mapped" || l_full_owner_status=$?
		fi
		if [ "$l_full_owner_status" -eq 0 ]; then
			zxfer_profile_stop_timer "$l_full_owner_destination_start_ms"
			g_zxfer_profile_destination_snapshot_listing_ms=$((g_zxfer_profile_destination_snapshot_listing_ms + g_zxfer_profile_elapsed_ms))
		fi
	fi

	# Validate the completed source before any deltas or caches are published.
	if [ "$l_full_owner_status" -eq 0 ]; then
		zxfer_echoV "Waiting for background processes to finish."
		l_full_owner_source_wait_status=0
		zxfer_wait_for_snapshot_discovery_producer "${g_source_snapshot_list_pid:-}" \
			"$l_full_owner_source_start_ms" source || l_full_owner_source_wait_status=$?
		g_source_snapshot_list_pid=""
		zxfer_validate_source_snapshot_wait_status "$l_full_owner_source_wait_status" "$l_full_workspace.error" || l_full_owner_status=$?
		if [ "$l_full_owner_status" -eq 0 ]; then
			zxfer_echoV "Background processes finished."
			if [ ! -s "$l_full_workspace" ]; then
				g_zxfer_snapshot_discovery_failure_result="Failed to retrieve snapshots from the source"
				l_full_owner_status=1
			fi
		fi
	fi
	if [ "$l_full_owner_status" -eq 0 ]; then
		l_full_owner_inventory_attempted=0
		zxfer_profile_start_timer
		l_full_owner_diff_start_ms=$g_zxfer_profile_clock_ms
		zxfer_set_g_recursive_source_list "$l_full_workspace.sorted" "$l_full_workspace_mapped" || l_full_owner_status=$?
		zxfer_profile_stop_timer "$l_full_owner_diff_start_ms"
		g_zxfer_profile_snapshot_diff_sort_ms=$((g_zxfer_profile_snapshot_diff_sort_ms + g_zxfer_profile_elapsed_ms))

		if [ "$l_full_owner_status" -eq 0 ] && zxfer_pass_has_replication_work; then
			zxfer_collect_destination_dataset_inventory || l_full_owner_status=$?
			l_full_owner_inventory_attempted=1
		fi
		# The same work needs both inventories and snapshot records. Nonrecursive
		# runs always retain records, even when the recursive diff found no work.
		if [ "$l_full_owner_status" -eq 0 ] &&
			{ [ "$l_full_owner_inventory_attempted" -eq 1 ] || [ -z "${g_option_R_recursive:-}" ]; }; then
			g_zxfer_destination_snapshot_record_cache_file=$l_full_workspace_destination
			if zxfer_get_temp_file; then
				g_zxfer_source_snapshot_record_cache_file=$g_zxfer_runtime_artifact_path_result
				if zxfer_command_trace_enabled; then
					zxfer_trace_rendered_command "Running command" \
						"$(zxfer_render_command_for_report "" zxfer_reverse_file_lines "$l_full_workspace") > $(zxfer_quote_token_for_report "$g_zxfer_source_snapshot_record_cache_file")"
				else
					zxfer_record_last_command_opaque
				fi
				zxfer_reverse_file_lines "$l_full_workspace" \
					>"$g_zxfer_source_snapshot_record_cache_file" || {
					l_full_owner_status=$?
					zxfer_throw_error "Failed to stage source snapshot record cache." "$l_full_owner_status"
				}
			else
				l_full_owner_status=$?
			fi
		fi
		if [ "$l_full_owner_status" -eq 0 ] && [ "$l_full_owner_inventory_attempted" -eq 1 ] &&
			[ "$g_recursive_dest_list" = "" ]; then
			zxfer_echoV "Destination dataset list is empty; assuming no existing datasets under \"$g_destination\""
		fi
	fi

	# Release only after the producer is reaped. On success, the destination's
	# raw listing may now be a planning cache; any reused sorted file is always
	# released, and failures discard both caches. Cleanup cannot replace the
	# first meaningful operation status.
	if [ -z "${g_source_snapshot_list_pid:-}" ]; then
		l_full_workspace_retained=""
		[ "$l_full_owner_status" -ne 0 ] ||
			l_full_workspace_retained=$g_zxfer_destination_snapshot_record_cache_file
		zxfer_cleanup_runtime_artifact_path_list "$l_full_workspace_paths" "$l_full_workspace_retained"
		[ "$l_full_owner_status" -eq 0 ] || zxfer_cleanup_snapshot_record_cache_files
	fi
	return "$l_full_owner_status"
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
