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
# owns globals: source-producer command/job state, staged-read/status channels,
#   parallel capability results, and origin parallel-command state.
# reads globals: source/destination ZFS commands, remote-origin settings,
#   compression helpers, parallel settings, and snapshot exclude options.
# mutates caches: producer-owned scratch only.
# returns via stdout: rendered source commands, bounded staged results, and
#   normalized snapshot streams; may launch registered producer processes.

# Purpose: Reset every source/destination producer result for a new discovery.
# Usage: Called by discovery orchestration instead of assigning producer scratch
# directly. The validated remote-parallel capability cache intentionally
# survives iteration resets and is cleared only by the session reset below.
zxfer_reset_snapshot_producer_state() {
	g_source_snapshot_list_cmd=""
	g_source_snapshot_list_pid=""
	g_source_snapshot_list_job_id=""
	g_source_snapshot_list_uses_parallel=0
	g_source_snapshot_list_uses_metadata_compression=0
	g_source_snapshot_list_sorted_file=""
	g_zxfer_snapshot_discovery_file_read_result=""
	g_zxfer_snapshot_discovery_status_file_result=""
	g_zxfer_parallel_source_job_check_result=""
	g_zxfer_parallel_source_job_check_kind=""
}

# Purpose: Reset producer state that is valid only within one zxfer session.
# Usage: Called once by the composition root before any discovery pass.
zxfer_reset_snapshot_producer_session_state() {
	zxfer_reset_snapshot_producer_state
	g_origin_parallel_cmd=""
	g_origin_parallel_cmd_host=""
}

# Purpose: Publish the source snapshot command selected by orchestration.
# Usage: Fast no-op and full discovery share the same producer-owned channel.
zxfer_set_source_snapshot_list_command() {
	g_source_snapshot_list_cmd=${1:-}
}

# Purpose: Clear the active producer PID and supervisor job identity.
# Usage: Called after wait/reap and before starting a new full discovery.
zxfer_clear_source_snapshot_list_job() {
	g_source_snapshot_list_pid=""
	g_source_snapshot_list_job_id=""
}

# Purpose: Clear the optional producer-side sorted output file.
# Usage: Called after ownership transfers to orchestration or cleanup finishes.
zxfer_clear_source_snapshot_list_sorted_file() {
	g_source_snapshot_list_sorted_file=""
}

# Purpose: Read the snapshot discovery capture file from staged state into the
# current shell.
# Usage: Called during source and destination snapshot discovery when later
# helpers need a checked reload instead of ad hoc file reads.
zxfer_read_snapshot_discovery_capture_file() {
	l_capture_path=$1

	g_zxfer_snapshot_discovery_file_read_result=""
	zxfer_read_runtime_artifact_file "$l_capture_path" >/dev/null || return "$?"
	g_zxfer_snapshot_discovery_file_read_result=$g_zxfer_runtime_artifact_read_result

	return 0
}

# Purpose: Read a staged source snapshot discovery command and normalize the
# final newline added by command-rendering helpers.
# Usage: Called during source snapshot discovery before executing or reporting
# the staged command.
zxfer_read_source_snapshot_discovery_command_file() {
	l_cmd_path=$1

	zxfer_read_snapshot_discovery_capture_file "$l_cmd_path" ||
		return "$?"
	case "$g_zxfer_snapshot_discovery_file_read_result" in
	*'
')
		g_zxfer_snapshot_discovery_file_read_result=${g_zxfer_snapshot_discovery_file_read_result%?}
		;;
	esac
}

# Purpose: Limit the snapshot discovery capture lines to the bounded form the
# surrounding flow expects.
# Usage: Called during source and destination snapshot discovery when zxfer
# needs a compact preview or bounded in-memory result.
zxfer_limit_snapshot_discovery_capture_lines() {
	l_capture_contents=$1
	l_line_limit=${2:-10}
	l_limited_contents=""
	l_line_count=0

	case "$l_line_limit" in
	'' | *[!0-9]*)
		l_line_limit=10
		;;
	esac
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

# Purpose: Check the parallel source jobs in current shell using the fail-
# closed rules owned by this module.
# Usage: Called during source and destination snapshot discovery before later
# helpers act on a result that must be validated first.
zxfer_check_parallel_source_jobs_in_current_shell() {
	g_zxfer_parallel_source_job_check_result=""
	g_zxfer_parallel_source_job_check_kind=""

	zxfer_ensure_parallel_available_for_source_jobs >/dev/null 2>&1
}

# Purpose: Ensure the parallel available for source jobs exists and is ready
# before the flow continues.
# Usage: Called during source and destination snapshot discovery before later
# helpers assume the resource or cache is available.
#
# Ensure parallel exists on the executing origin host. zxfer intentionally
# trusts the resolved helper instead of version/banner probing it; the rendered
# discovery pipeline uses GNU-compatible options and will fail if the helper is
# not compatible.
# Once the user requests -j source discovery, zxfer must stay on the parallel
# path instead of silently dropping back to the serial source listing.
zxfer_ensure_parallel_available_for_source_jobs() {
	g_zxfer_parallel_source_job_check_result=""
	g_zxfer_parallel_source_job_check_kind=""

	if [ "$g_option_j_jobs" -le 1 ]; then
		return 0
	fi

	if [ "$g_option_O_origin_host" = "" ]; then
		if [ "$g_cmd_parallel" = "" ]; then
			g_zxfer_parallel_source_job_check_kind="local_missing"
			g_zxfer_parallel_source_job_check_result="The -j option requires parallel but it was not found in PATH on the local host."
			printf '%s\n' "$g_zxfer_parallel_source_job_check_result"
			return 1
		fi

		return 0
	fi

	if [ -n "${g_origin_parallel_cmd:-}" ] &&
		[ "${g_origin_parallel_cmd_host:-}" = "$g_option_O_origin_host" ]; then
		return 0
	fi

	l_remote_parallel_status=0
	l_remote_parallel=$(zxfer_resolve_remote_required_tool "$g_option_O_origin_host" parallel "parallel" source) ||
		l_remote_parallel_status=$?
	if [ "$l_remote_parallel_status" -ne 0 ]; then
		case "$l_remote_parallel" in
		"Required dependency \"parallel\" not found on host "*)
			g_zxfer_parallel_source_job_check_kind="origin_missing"
			g_zxfer_parallel_source_job_check_result="parallel not found on origin host $g_option_O_origin_host but -j $g_option_j_jobs was requested. Install parallel remotely or rerun without -j."
			;;
		*)
			g_zxfer_parallel_source_job_check_kind="origin_probe_failed"
			g_zxfer_parallel_source_job_check_result=$l_remote_parallel
			;;
		esac
		printf '%s\n' "$g_zxfer_parallel_source_job_check_result"
		return "$l_remote_parallel_status"
	fi

	g_origin_parallel_cmd=$l_remote_parallel
	g_origin_parallel_cmd_host=$g_option_O_origin_host

	return 0
}

# Purpose: Return the shell-safe remote metadata compression command.
# Usage: Called by remote source snapshot discovery after the origin host has
# already been initialized or preloaded, preserving secure helper resolution.
# Snapshot-list metadata reuses the operator-selected send/receive compressor
# so both streams pay the same compression cost.
zxfer_get_origin_metadata_compress_safe() {
	if [ "${g_option_z_compress:-0}" -ne 1 ]; then
		return 1
	fi
	l_metadata_compress_cmd=${g_cmd_compress:-}
	[ -n "$l_metadata_compress_cmd" ] || return 1

	if [ -n "${g_origin_cmd_compress_safe:-}" ]; then
		printf '%s\n' "$g_origin_cmd_compress_safe"
		return 0
	fi

	zxfer_resolve_remote_cli_command_safe \
		"$g_option_O_origin_host" "$l_metadata_compress_cmd" \
		"metadata compression command" source
}

# Purpose: Return the awk program that filters snapshot records by dataset.
# Usage: Shared by file-backed and streaming exclude paths so `-x` keeps the
# same dataset-name semantics in fast no-op and full recursive planning.
zxfer_get_snapshot_exclude_filter_awk_program() {
	printf '%s\n' "{ snapshot_path = \$0; if (substr(snapshot_path, 1, 1) == \"\\t\") { snapshot_path = substr(snapshot_path, 2) }; tab_pos = index(snapshot_path, \"\\t\"); if (tab_pos > 0) { snapshot_path = substr(snapshot_path, 1, tab_pos - 1) }; at_pos = index(snapshot_path, \"@\"); snapshot_dataset = snapshot_path; if (at_pos > 0) { snapshot_dataset = substr(snapshot_path, 1, at_pos - 1) }; if (snapshot_dataset !~ exclude_pattern) { print } }"
}

# Purpose: Build the source snapshot-identity list command used by the
# recursive no-op proof.
# Usage: Called before the full creation-order source discovery so clean no-op
# runs can avoid creation-order source discovery. The proof deliberately uses
# one recursive source stream even when `-j` is configured. The full
# changed-source discovery path still honors `-j`; the proof avoids multiplying
# GNU parallel and per-dataset `zfs list` startup cost before it knows there is
# work to do. Keep GUIDs here so exact-name snapshot divergence cannot be
# treated as a no-op.
zxfer_build_source_snapshot_name_list_cmd() {
	g_source_snapshot_list_uses_parallel=0
	g_source_snapshot_list_uses_metadata_compression=0

	if [ "$g_option_O_origin_host" = "" ]; then
		zxfer_render_zfs_command_for_spec "$g_LZFS" list -Hr -o name,guid -t snapshot "$g_initial_source"
		return
	fi

	l_remote_zfs_cmd=${g_origin_cmd_zfs:-$g_cmd_zfs}
	l_remote_pipeline=$(zxfer_build_shell_command_from_argv \
		"$l_remote_zfs_cmd" list -Hr -o name,guid -t snapshot "$g_initial_source") ||
		return "$?"
	if [ "${g_option_z_compress:-0}" -eq 1 ]; then
		g_source_snapshot_list_uses_metadata_compression=1
		l_remote_compress_safe=$(zxfer_get_origin_metadata_compress_safe) ||
			return "$?"
		# The remote compressor masks the listing's exit status (no pipefail
		# in the remote sh), so gate a success sentinel on the listing and
		# verify/strip it locally — otherwise a listing that died mid-stream
		# would surface as a truncated-but-successful result and could let the
		# fast no-op proof falsely conclude nothing needs to transfer.
		l_sentinel_line=$(zxfer_get_source_discovery_sentinel_line)
		l_remote_pipeline="{ $l_remote_pipeline && printf '%s\n' '$l_sentinel_line'; } | $l_remote_compress_safe"
	fi
	l_remote_shell_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_pipeline") ||
		return "$?"
	l_cmd=$(zxfer_build_ssh_shell_command_for_host "$g_option_O_origin_host" "$l_remote_shell_cmd") ||
		return "$?"
	if [ "${g_source_snapshot_list_uses_metadata_compression:-0}" -eq 1 ]; then
		l_sentinel_filter_cmd=$(zxfer_build_discovery_sentinel_filter_cmd) ||
			return "$?"
		l_cmd="$l_cmd | $g_cmd_decompress_safe | $l_sentinel_filter_cmd"
	fi

	printf '%s\n' "$l_cmd"
}

# Purpose: Launch a source snapshot command and sort its output in the
# background without preserving a creation-order sidecar.
# Usage: Called by the recursive no-op proof where only sorted identity
# comparison data is needed.
# Side effects: When a count-file path is passed as the fourth argument, writes
# 1 when at least one post-filter snapshot reached sort, otherwise 0.
zxfer_execute_source_snapshot_name_list_background_sort_cmd() {
	l_cmd=$1
	l_sorted_output_file=$2
	l_error_file=${3:-}
	l_count_file=${4:-}

	l_cleanup_wrapper_script=$(zxfer_get_cleanup_child_wrapper_script_path) || return 1
	l_sorted_output_file_safe=$(zxfer_build_shell_command_from_argv "$l_sorted_output_file") ||
		return "$?"
	zxfer_get_temp_file >/dev/null || return "$?"
	l_source_status_file=$g_zxfer_temp_file_result
	l_source_status_file_safe=$(zxfer_build_shell_command_from_argv "$l_source_status_file") || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_source_status_file"
		return "$l_status"
	}
	l_count_status_file=""
	l_count_file_safe=""
	l_count_status_file_safe=""
	l_count_stage_cmd=""
	if [ -n "$l_count_file" ]; then
		l_count_file_safe=$(zxfer_build_shell_command_from_argv "$l_count_file") || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_source_status_file"
			return "$l_status"
		}
		zxfer_get_temp_file >/dev/null || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_source_status_file"
			return "$l_status"
		}
		l_count_status_file=$g_zxfer_temp_file_result
		l_count_status_file_safe=$(zxfer_build_shell_command_from_argv "$l_count_status_file") || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths "$l_source_status_file" "$l_count_status_file"
			return "$l_status"
		}
		l_count_stage_cmd="{ l_count_status=0; if IFS= read -r l_first_snapshot; then printf '%s\n' 1 > $l_count_file_safe || l_count_status=\$?; printf '%s\n' \"\$l_first_snapshot\" || l_count_status=\$?; cat || l_count_status=\$?; else printf '%s\n' 0 > $l_count_file_safe || l_count_status=\$?; fi; printf '%s\n' \"\$l_count_status\" > $l_count_status_file_safe; exit \"\$l_count_status\"; }"
	fi
	if [ -n "${g_option_x_exclude_datasets:-}" ]; then
		zxfer_get_temp_file >/dev/null || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths "$l_source_status_file" "$l_count_status_file"
			return "$l_status"
		}
		l_filter_status_file=$g_zxfer_temp_file_result
		l_filter_status_file_safe=$(zxfer_build_shell_command_from_argv "$l_filter_status_file") || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths "$l_source_status_file" "$l_filter_status_file" "$l_count_status_file"
			return "$l_status"
		}
		l_filter_program=$(zxfer_get_snapshot_exclude_filter_awk_program)
		l_filter_cmd=$(zxfer_build_shell_command_from_argv \
			"${g_cmd_awk:-awk}" \
			-v "exclude_pattern=$g_option_x_exclude_datasets" \
			"$l_filter_program") ||
			l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_paths "$l_source_status_file" "$l_filter_status_file" "$l_count_status_file"
			return "$l_status"
		fi
		if [ -n "$l_count_stage_cmd" ]; then
			l_managed_cmd="{ ( $l_cmd ); printf '%s\n' \"\$?\" > $l_source_status_file_safe; } | { $l_filter_cmd; printf '%s\n' \"\$?\" > $l_filter_status_file_safe; } | $l_count_stage_cmd | LC_ALL=C sort > $l_sorted_output_file_safe; l_sort_status=\$?; l_source_status=1; l_filter_status=1; l_count_status=1; if [ -f $l_source_status_file_safe ]; then IFS= read -r l_source_status < $l_source_status_file_safe || l_source_status=1; fi; if [ -f $l_filter_status_file_safe ]; then IFS= read -r l_filter_status < $l_filter_status_file_safe || l_filter_status=1; fi; if [ -f $l_count_status_file_safe ]; then IFS= read -r l_count_status < $l_count_status_file_safe || l_count_status=1; fi; rm -f $l_source_status_file_safe $l_filter_status_file_safe $l_count_status_file_safe; case \"\$l_source_status:\$l_filter_status:\$l_count_status:\$l_sort_status\" in *[!0-9:]*) exit 1 ;; esac; [ \"\$l_source_status\" -eq 0 ] || exit \"\$l_source_status\"; [ \"\$l_filter_status\" -eq 0 ] || exit \"\$l_filter_status\"; [ \"\$l_count_status\" -eq 0 ] || exit \"\$l_count_status\"; exit \"\$l_sort_status\""
		else
			l_managed_cmd="{ ( $l_cmd ); printf '%s\n' \"\$?\" > $l_source_status_file_safe; } | { $l_filter_cmd; printf '%s\n' \"\$?\" > $l_filter_status_file_safe; } | LC_ALL=C sort > $l_sorted_output_file_safe; l_sort_status=\$?; l_source_status=1; l_filter_status=1; if [ -f $l_source_status_file_safe ]; then IFS= read -r l_source_status < $l_source_status_file_safe || l_source_status=1; fi; if [ -f $l_filter_status_file_safe ]; then IFS= read -r l_filter_status < $l_filter_status_file_safe || l_filter_status=1; fi; rm -f $l_source_status_file_safe $l_filter_status_file_safe; case \"\$l_source_status:\$l_filter_status:\$l_sort_status\" in *[!0-9:]*) exit 1 ;; esac; [ \"\$l_source_status\" -eq 0 ] || exit \"\$l_source_status\"; [ \"\$l_filter_status\" -eq 0 ] || exit \"\$l_filter_status\"; exit \"\$l_sort_status\""
		fi
	else
		if [ -n "$l_count_stage_cmd" ]; then
			l_managed_cmd="{ ( $l_cmd ); printf '%s\n' \"\$?\" > $l_source_status_file_safe; } | $l_count_stage_cmd | LC_ALL=C sort > $l_sorted_output_file_safe; l_sort_status=\$?; l_source_status=1; l_count_status=1; if [ -f $l_source_status_file_safe ]; then IFS= read -r l_source_status < $l_source_status_file_safe || l_source_status=1; fi; if [ -f $l_count_status_file_safe ]; then IFS= read -r l_count_status < $l_count_status_file_safe || l_count_status=1; fi; rm -f $l_source_status_file_safe $l_count_status_file_safe; case \"\$l_source_status:\$l_count_status:\$l_sort_status\" in *[!0-9:]*) exit 1 ;; esac; [ \"\$l_source_status\" -eq 0 ] || exit \"\$l_source_status\"; [ \"\$l_count_status\" -eq 0 ] || exit \"\$l_count_status\"; exit \"\$l_sort_status\""
		else
			l_managed_cmd="{ ( $l_cmd ); printf '%s\n' \"\$?\" > $l_source_status_file_safe; } | LC_ALL=C sort > $l_sorted_output_file_safe; l_sort_status=\$?; l_source_status=1; if [ -f $l_source_status_file_safe ]; then IFS= read -r l_source_status < $l_source_status_file_safe || l_source_status=1; fi; rm -f $l_source_status_file_safe; case \"\$l_source_status:\$l_sort_status\" in *[!0-9:]*) exit 1 ;; esac; [ \"\$l_source_status\" -eq 0 ] || exit \"\$l_source_status\"; exit \"\$l_sort_status\""
		fi
	fi

	zxfer_echoV "Executing command in the background: $l_managed_cmd"
	zxfer_record_last_command_string "$l_managed_cmd"
	if [ -n "$l_error_file" ]; then
		"$l_cleanup_wrapper_script" "$l_managed_cmd" >/dev/null 2>"$l_error_file" &
	else
		"$l_cleanup_wrapper_script" "$l_managed_cmd" >/dev/null &
	fi
	# shellcheck disable=SC2034
	zxfer_set_last_background_pid "$!"
	if ! zxfer_register_cleanup_pid \
		"$g_last_background_pid" "background source snapshot no-op proof helper"; then
		if ! zxfer_abort_direct_child_pid \
			"$g_last_background_pid" TERM \
			"background source snapshot no-op proof helper"; then
			return 1
		fi
		wait "$g_last_background_pid" 2>/dev/null || :
		zxfer_unregister_cleanup_pid "$g_last_background_pid"
		zxfer_clear_last_background_pid
		return 1
	fi

	return 0
}

# Purpose: Return the sentinel line appended to parallel discovery output on
# success so a partially failed listing can never be mistaken for a complete
# one.
# Usage: Called by the parallel discovery command builders and the sentinel
# filter so both sides agree on one constant.
zxfer_get_source_discovery_sentinel_line() {
	printf '%s\n' '@@ZXFER_SOURCE_SNAPSHOT_DISCOVERY_COMPLETE@@'
}

# Purpose: Build the local filter that verifies and strips the parallel
# discovery success sentinel.
# Usage: Appended as the final local stage of parallel discovery pipelines.
# The filter removes the trailing sentinel line from the stream and exits 65
# when it is absent, which the discovery wait path reports as a fatal
# incomplete-listing error. Without this, a failed per-dataset sub-listing
# inside `parallel` is masked by later pipeline stages (zstd or the remote
# shell's last-command status) and zxfer would silently plan against a partial
# snapshot list.
zxfer_build_discovery_sentinel_filter_cmd() {
	l_sentinel_line=$(zxfer_get_source_discovery_sentinel_line)

	# shellcheck disable=SC2016  # awk program must see literal $0.
	zxfer_build_shell_command_from_argv "${g_cmd_awk:-awk}" \
		-v sentinel_line="$l_sentinel_line" \
		'NR > 1 { print prev_line } { prev_line = $0 } END { if (NR == 0 || prev_line != sentinel_line) exit 65 }'
}

# Purpose: Build the source snapshot list command for the next execution or
# comparison step.
# Usage: Called during source and destination snapshot discovery before other
# helpers consume the assembled value.
#
# Build the ZFS list command used to enumerate source snapshots based on the
# current CLI state. Separating this from execution allows tests to assert on
# the constructed pipeline without invoking ZFS.
#
# Parallel variants stage their pipelines so every failure is observable:
# the dataset enumeration is captured first (exit 70 on failure), and the
# per-dataset `parallel` listing must succeed before the success sentinel is
# emitted. The local sentinel filter then strips the sentinel and fails the
# whole discovery when it is missing.
zxfer_build_source_snapshot_list_cmd() {
	g_source_snapshot_list_uses_parallel=0
	g_source_snapshot_list_uses_metadata_compression=0
	l_local_serial_cmd=$(zxfer_render_zfs_command_for_spec "$g_LZFS" list -Hr -o name,guid -s creation -t snapshot "$g_initial_source") ||
		return "$?"

	if [ "$g_option_j_jobs" -le 1 ]; then
		printf '%s\n' "$l_local_serial_cmd"
		return
	fi

	if zxfer_check_parallel_source_jobs_in_current_shell; then
		l_parallel_status=0
	else
		l_parallel_status=$?
	fi
	if [ "$l_parallel_status" -ne 0 ]; then
		if [ -n "${g_zxfer_parallel_source_job_check_result:-}" ]; then
			printf '%s\n' "$g_zxfer_parallel_source_job_check_result"
		else
			printf '%s\n' "Failed to prepare parallel source discovery."
		fi
		return "$l_parallel_status"
	fi

	g_source_snapshot_list_uses_parallel=1

	if [ ! "$g_option_O_origin_host" = "" ]; then
		l_parallel_path=$g_origin_parallel_cmd
		l_remote_zfs_cmd=${g_origin_cmd_zfs:-$g_cmd_zfs}
		l_parallel_cmd=$(zxfer_build_shell_command_from_argv "$l_parallel_path") || return "$?"
		if l_remote_runner_cmd=$(zxfer_build_shell_command_from_argv \
			"$l_remote_zfs_cmd" list -H -o name,guid -s creation -d 1 -t snapshot "{}"); then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		l_remote_parallel_cmd="$l_parallel_cmd -j $g_option_j_jobs --line-buffer -- \"$l_remote_runner_cmd\""
		if l_remote_dataset_input_cmd=$(zxfer_build_shell_command_from_argv \
			"$l_remote_zfs_cmd" list -Hr -t filesystem,volume -o name "$g_initial_source"); then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		l_sentinel_line=$(zxfer_get_source_discovery_sentinel_line)
		# Capture the dataset enumeration before fanning out: piped directly
		# into `parallel`, a failed enumeration would look like an empty
		# dataset list, parallel would succeed, and the sentinel would falsely
		# mark the listing complete. Note the remote exit codes (70, or
		# parallel's own status) are masked locally by the later pipeline
		# stages (no pipefail); the failure reliably surfaces as the missing
		# sentinel (filter exit 65) plus the remote stderr captured for the
		# discovery error report.
		l_remote_pipeline="zxfer_discovery_datasets=\$($l_remote_dataset_input_cmd) || exit 70; { printf '%s\n' \"\$zxfer_discovery_datasets\" | $l_remote_parallel_cmd && printf '%s\n' '$l_sentinel_line'; }"
		if [ "$g_option_z_compress" -eq 1 ]; then
			g_source_snapshot_list_uses_metadata_compression=1
			l_remote_compress_safe=$(zxfer_get_origin_metadata_compress_safe) ||
				return "$?"
			l_remote_pipeline="$l_remote_pipeline | $l_remote_compress_safe"
		fi
		l_remote_shell_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_pipeline") || return "$?"
		l_cmd=$(zxfer_build_ssh_shell_command_for_host "$g_option_O_origin_host" "$l_remote_shell_cmd") ||
			return "$?"
		if [ "${g_source_snapshot_list_uses_metadata_compression:-0}" -eq 1 ]; then
			l_cmd="$l_cmd | $g_cmd_decompress_safe"
		fi
		l_sentinel_filter_cmd=$(zxfer_build_discovery_sentinel_filter_cmd) || return "$?"
		l_cmd="$l_cmd | $l_sentinel_filter_cmd"
		printf '%s\n' "$l_cmd"
		return
	fi

	l_parallel_path=$g_cmd_parallel
	l_runner_cmd=$(zxfer_render_zfs_command_for_spec "$g_LZFS" list -H -o name,guid -s creation -d 1 -t snapshot "{}") ||
		return "$?"
	l_parallel_cmd=$(zxfer_build_shell_command_from_argv "$l_parallel_path") || return "$?"
	l_parallel_cmd="$l_parallel_cmd -j $g_option_j_jobs --line-buffer -- \"$l_runner_cmd\""
	if l_dataset_input_cmd=$(zxfer_render_zfs_command_for_spec \
		"$g_LZFS" list -Hr -t filesystem,volume -o name "$g_initial_source"); then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_sentinel_line=$(zxfer_get_source_discovery_sentinel_line)
	l_sentinel_filter_cmd=$(zxfer_build_discovery_sentinel_filter_cmd) || return "$?"
	# Same staging as the remote variant. Locally the wrapper shell runs this
	# command list directly, so enumeration failures surface as exit 70, and a
	# failed parallel sub-listing withholds the sentinel so the filter fails
	# the pipeline with exit 65.
	l_cmd="zxfer_discovery_datasets=\$($l_dataset_input_cmd) || exit 70; { printf '%s\n' \"\$zxfer_discovery_datasets\" | $l_parallel_cmd && printf '%s\n' '$l_sentinel_line'; } | $l_sentinel_filter_cmd"
	printf '%s\n' "$l_cmd"
}

# Purpose: Launch source snapshot discovery and sort its output inside the
# same background job.
# Usage: Called by zxfer_write_source_snapshot_list_to_file when recursive
# discovery can overlap the source-list sort with destination discovery work.
# Side effects: Publishes the background helper pid in g_last_background_pid.
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

	l_source_background_cleanup_wrapper=$(zxfer_get_cleanup_child_wrapper_script_path) ||
		return 1
	l_output_file_safe=$(zxfer_build_shell_command_from_argv \
		"$l_source_background_output_file") ||
		return "$?"
	l_sorted_output_file_safe=$(zxfer_build_shell_command_from_argv "$l_sorted_output_file") ||
		return "$?"
	zxfer_create_temp_file_group 2 >/dev/null || return "$?"
	l_sort_status_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_source_status_file
		IFS= read -r l_tee_status_file
	} <<-EOF
		$l_sort_status_stage_files
	EOF
	l_source_status_file_safe=$(zxfer_build_shell_command_from_argv "$l_source_status_file") || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path_list "$l_sort_status_stage_files"
		return "$l_status"
	}
	l_tee_status_file_safe=$(zxfer_build_shell_command_from_argv "$l_tee_status_file") || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path_list "$l_sort_status_stage_files"
		return "$l_status"
	}
	l_managed_cmd="{ ( $l_source_background_command ); printf '%s\n' \"\$?\" > $l_source_status_file_safe; } | { tee $l_output_file_safe; printf '%s\n' \"\$?\" > $l_tee_status_file_safe; } | LC_ALL=C sort > $l_sorted_output_file_safe; l_sort_status=\$?; l_source_status=1; l_tee_status=1; if [ -f $l_source_status_file_safe ]; then IFS= read -r l_source_status < $l_source_status_file_safe || l_source_status=1; fi; if [ -f $l_tee_status_file_safe ]; then IFS= read -r l_tee_status < $l_tee_status_file_safe || l_tee_status=1; fi; rm -f $l_source_status_file_safe $l_tee_status_file_safe; case \"\$l_source_status:\$l_tee_status:\$l_sort_status\" in *[!0-9:]*) exit 1 ;; esac; [ \"\$l_source_status\" -eq 0 ] || exit \"\$l_source_status\"; [ \"\$l_tee_status\" -eq 0 ] || exit \"\$l_tee_status\"; exit \"\$l_sort_status\""

	zxfer_echoV "Executing command in the background: $l_managed_cmd"
	zxfer_record_last_command_string "$l_managed_cmd"
	if [ -n "$l_source_background_error_file" ]; then
		"$l_source_background_cleanup_wrapper" \
			"$l_managed_cmd" >/dev/null 2>"$l_source_background_error_file" &
	else
		"$l_source_background_cleanup_wrapper" "$l_managed_cmd" >/dev/null &
	fi
	# shellcheck disable=SC2034
	zxfer_set_last_background_pid "$!"
	if ! zxfer_register_cleanup_pid \
		"$g_last_background_pid" "background source snapshot discovery helper"; then
		if ! zxfer_abort_direct_child_pid \
			"$g_last_background_pid" TERM \
			"background source snapshot discovery helper"; then
			return 1
		fi
		wait "$g_last_background_pid" 2>/dev/null || :
		zxfer_unregister_cleanup_pid "$g_last_background_pid"
		zxfer_clear_last_background_pid
		return 1
	fi

	return 0
}

# Purpose: Write the source snapshot list to file in the normalized form later
# zxfer steps expect.
# Usage: Called during source and destination snapshot discovery when the
# module needs a stable staged file or emitted stream for downstream use.
#
# Determine the source snapshots sorted by creation time. Since this
# can take a long time, the command is run in the background. In addition,
# to optimize the process, parallel is used to retrieve snapshots from
# multiple datasets concurrently.
zxfer_write_source_snapshot_list_to_file() {
	l_outfile=$1
	l_errfile=${2:-}
	l_cmd_tmp_file=""
	l_sorted_outfile=""
	zxfer_profile_increment_counter g_zxfer_profile_source_snapshot_list_commands
	zxfer_profile_record_bucket source_inspection

	#
	# it is important to get this in ascending order because when getting
	# in descending order, the datasets names are not ordered as we want.
	# Don't use -S creation for this command, instead, reverse the results below
	#
	g_source_snapshot_list_uses_parallel=0
	g_source_snapshot_list_uses_metadata_compression=0

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		l_source_snapshot_command=$(zxfer_render_zfs_command_for_spec \
			"$g_LZFS" list -Hr -o name,guid -s creation -t snapshot \
			"$g_initial_source") ||
			zxfer_throw_error "${l_source_snapshot_command:-Failed to render dry-run source snapshot discovery command.}" "$?"
		g_source_snapshot_list_cmd=$l_source_snapshot_command
		zxfer_echoV "Dry run: $l_source_snapshot_command"
		zxfer_record_last_command_string "$l_source_snapshot_command"
		zxfer_write_runtime_artifact_file "$l_outfile" "" || return "$?"
		if [ -n "$l_errfile" ]; then
			zxfer_write_runtime_artifact_file "$l_errfile" "" || return "$?"
		fi
		if [ "${g_source_snapshot_list_background_sort_requested:-0}" -eq 1 ]; then
			zxfer_get_temp_file >/dev/null || return "$?"
			g_source_snapshot_list_sorted_file=$g_zxfer_temp_file_result
			zxfer_write_runtime_artifact_file "$g_source_snapshot_list_sorted_file" "" ||
				return "$?"
		fi
		g_source_snapshot_list_pid=""
		g_source_snapshot_list_job_id=""
		return 0
	fi

	zxfer_get_temp_file >/dev/null || return "$?"
	l_cmd_tmp_file=$g_zxfer_temp_file_result
	l_write_source_snapshot_list_to_file_status=0
	zxfer_build_source_snapshot_list_cmd >"$l_cmd_tmp_file" || l_write_source_snapshot_list_to_file_status=$?
	l_read_status=0
	zxfer_read_source_snapshot_discovery_command_file "$l_cmd_tmp_file" || l_read_status=$?
	l_source_snapshot_command=$g_zxfer_snapshot_discovery_file_read_result
	zxfer_cleanup_runtime_artifact_path "$l_cmd_tmp_file"
	if [ "$l_read_status" -ne 0 ]; then
		if [ "$l_write_source_snapshot_list_to_file_status" -ne 0 ]; then
			zxfer_throw_error "Failed to read staged source snapshot discovery command after build failure." "$l_read_status"
		fi
		zxfer_throw_error "Failed to read staged source snapshot discovery command." "$l_read_status"
	fi
	if [ "$l_write_source_snapshot_list_to_file_status" -ne 0 ]; then
		zxfer_throw_error "${l_source_snapshot_command:-Failed to build source snapshot discovery command.}" "$l_write_source_snapshot_list_to_file_status"
	fi
	[ -n "$l_source_snapshot_command" ] ||
		zxfer_throw_error "Staged source snapshot discovery command was empty."
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
		zxfer_get_temp_file >/dev/null || return "$?"
		l_sorted_outfile=$g_zxfer_temp_file_result
		g_source_snapshot_list_sorted_file=$l_sorted_outfile
		if zxfer_execute_source_snapshot_list_background_cmd_with_sort \
			"$l_source_snapshot_command" "$l_outfile" \
			"$l_errfile" "$l_sorted_outfile"; then
			:
		else
			l_write_source_snapshot_list_to_file_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_sorted_outfile"
			g_source_snapshot_list_sorted_file=""
			return "$l_write_source_snapshot_list_to_file_status"
		fi
	else
		zxfer_execute_rendered_background_shell_command \
			"$l_source_snapshot_command" "$l_outfile" "$l_errfile" ||
			return "$?"
	fi
	g_source_snapshot_list_pid=$g_last_background_pid
	g_source_snapshot_list_job_id=""
}

# Purpose: Normalize the destination snapshot list into the stable form used
# across zxfer.
# Usage: Called during source and destination snapshot discovery before
# comparison, caching, or reporting depends on exact formatting.
#
# Normalize the destination snapshot list into source-path form so it can be
# directly compared to the source listing via comm. Trailing-slash replication
# changes the destination root, but destination snapshot records still need the
# same prefix rewrite before identity comparison.
zxfer_normalize_destination_snapshot_list() {
	l_destination_dataset=$1
	l_input_file=$2
	l_output_file=$3

	# shellcheck disable=SC2016  # awk program should see literal $0.
	l_prefix_rewrite_program='
{
	if (index($0, destination_dataset) == 1) {
		suffix = substr($0, length(destination_dataset) + 1)
		if (substr(suffix, 1, 1) == "@" || substr(suffix, 1, 1) == "/") {
			print initial_source suffix
			next
		}
	}
	print
}'
	if zxfer_command_display_render_enabled; then
		l_cmd="$(zxfer_render_command_for_report "" "${g_cmd_awk:-awk}" \
			-v "destination_dataset=$l_destination_dataset" \
			-v "initial_source=$g_initial_source" \
			"$l_prefix_rewrite_program" \
			"$l_input_file") | $(zxfer_render_command_for_report "LC_ALL=C" sort) > $(zxfer_quote_token_for_report "$l_output_file")"
		zxfer_echoV "Running command: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
	else
		zxfer_record_last_command_opaque
	fi
	zxfer_get_temp_file >/dev/null || return "$?"
	l_awk_status_file=$g_zxfer_temp_file_result
	{
		"${g_cmd_awk:-awk}" \
			-v "destination_dataset=$l_destination_dataset" \
			-v "initial_source=$g_initial_source" \
			"$l_prefix_rewrite_program" \
			"$l_input_file"
		printf '%s\n' "$?" >"$l_awk_status_file" 2>/dev/null || :
	} | LC_ALL=C sort >"$l_output_file"
	l_sort_status=$?
	l_awk_status=1
	if [ -f "$l_awk_status_file" ]; then
		IFS= read -r l_awk_status <"$l_awk_status_file" || l_awk_status=1
	fi
	zxfer_cleanup_runtime_artifact_path "$l_awk_status_file"
	case "$l_awk_status" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	if [ "$l_awk_status" -ne 0 ]; then
		return "$l_awk_status"
	fi
	return "$l_sort_status"
}

# Purpose: Normalize destination snapshots from stdin for the fast no-op proof.
# Usage: Called while streaming the destination snapshot list into the
# canonical byte-order sort used by the proof, avoiding a raw full-list staging
# file that the proof cannot reuse.
zxfer_normalize_destination_snapshot_stream_for_noop_proof() {
	l_destination_dataset=$1

	if [ -z "${g_option_x_exclude_datasets:-}" ]; then
		# shellcheck disable=SC2016  # awk program should see literal $0.
		l_prefix_rewrite_program='
{
	if (index($0, destination_dataset) == 1) {
		suffix = substr($0, length(destination_dataset) + 1)
		if (substr(suffix, 1, 1) == "@" || substr(suffix, 1, 1) == "/") {
			print initial_source suffix
			next
		}
	}
	print
}'
		"${g_cmd_awk:-awk}" \
			-v "destination_dataset=$l_destination_dataset" \
			-v "initial_source=$g_initial_source" \
			"$l_prefix_rewrite_program"
		return "$?"
	fi

	# shellcheck disable=SC2016  # awk program should see literal $0.
	l_prefix_rewrite_program='
{
	if (index($0, destination_dataset) == 1) {
		suffix = substr($0, length(destination_dataset) + 1)
		if (substr(suffix, 1, 1) == "@" || substr(suffix, 1, 1) == "/") {
			snapshot_path = initial_source suffix
		} else {
			snapshot_path = $0
		}
	} else {
		snapshot_path = $0
	}
	filter_path = snapshot_path
	tab_pos = index(filter_path, "\t")
	if (tab_pos > 0) {
		filter_path = substr(filter_path, 1, tab_pos - 1)
	}
	at_pos = index(filter_path, "@")
	snapshot_dataset = filter_path
	if (at_pos > 0) {
		snapshot_dataset = substr(filter_path, 1, at_pos - 1)
	}
	if (snapshot_dataset !~ exclude_pattern) {
		print snapshot_path
	}
}'
	"${g_cmd_awk:-awk}" \
		-v "destination_dataset=$l_destination_dataset" \
		-v "initial_source=$g_initial_source" \
		-v "exclude_pattern=$g_option_x_exclude_datasets" \
		"$l_prefix_rewrite_program"
}

# Purpose: Read a small numeric status sidecar written by a snapshot discovery
# pipeline.
# Usage: Called after background streaming producers finish so zxfer validates
# command status without loading large snapshot streams into shell variables.
# Side effects: Publishes the status in
# g_zxfer_snapshot_discovery_status_file_result.
zxfer_read_snapshot_discovery_status_file() {
	l_status_file=$1
	l_default_status=${2:-1}

	g_zxfer_snapshot_discovery_status_file_result=$l_default_status
	if [ -f "$l_status_file" ]; then
		IFS= read -r g_zxfer_snapshot_discovery_status_file_result <"$l_status_file" ||
			g_zxfer_snapshot_discovery_status_file_result=$l_default_status
	fi
	case "$g_zxfer_snapshot_discovery_status_file_result" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	return 0
}

# Purpose: Publish destination dataset inventory from staged files into the
# shared discovery state.
# Usage: Called by local and remote destination discovery after inventory
# commands have completed so missing-root bootstrap and failure handling stay
# identical.
zxfer_publish_destination_dataset_inventory_from_stage() {
	l_destination_inventory_tmp_file=$1
	l_destination_inventory_err_file=$2
	l_destination_inventory_status=$3
	l_destination_inventory_pool_status=${4:-}

	if [ "$l_destination_inventory_status" -eq 0 ]; then
		zxfer_read_snapshot_discovery_capture_file \
			"$l_destination_inventory_tmp_file" ||
			zxfer_throw_error "Failed to read staged destination dataset inventory." "$?"
		zxfer_set_recursive_destination_list "$g_zxfer_snapshot_discovery_file_read_result"
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
			zxfer_set_recursive_destination_list ""
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

# Purpose: Write the destination snapshot list to files in the normalized form
# later zxfer steps expect.
# Usage: Called during source and destination snapshot discovery when the
# module needs a stable staged file or emitted stream for downstream use.
#
# We only need the snapshots of the intended destination dataset, not
# all the snapshots of the parent $g_destination.
# In addition, sorting by creation time has been removed in the
# destination since it is not needed.
# This significantly improves performance as the metadata
# doesn't need to be searched for the creation time of each snapshot.
# Parallelization support has been added and is useful in situations when
# the ARC is not populated such as when a removable disk is mounted.
zxfer_write_destination_snapshot_list_to_files() {
	l_rzfs_list_hr_snap_tmp_file=$1
	l_dest_snaps_stripped_sorted_tmp_file=$2

	l_write_destination_snapshot_list_to_files_destination_dataset=$(zxfer_get_destination_snapshot_root_dataset)

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		l_write_destination_snapshot_list_to_files_cmd=$(zxfer_render_destination_zfs_command list -Hr -o name,guid -t snapshot "$l_write_destination_snapshot_list_to_files_destination_dataset") ||
			zxfer_throw_error "${l_write_destination_snapshot_list_to_files_cmd:-Failed to render dry-run destination snapshot discovery command.}" "$?"
		zxfer_echoV "Dry run: $l_write_destination_snapshot_list_to_files_cmd"
		zxfer_record_last_command_string "$l_write_destination_snapshot_list_to_files_cmd"
		zxfer_write_runtime_artifact_file "$l_rzfs_list_hr_snap_tmp_file" "" || return "$?"
		zxfer_write_runtime_artifact_file "$l_dest_snaps_stripped_sorted_tmp_file" "" || return "$?"
		return
	fi

	# check if the destination zfs dataset exists before listing snapshots
	l_destination_exists=$(zxfer_exists_destination "$l_write_destination_snapshot_list_to_files_destination_dataset") ||
		zxfer_throw_error "$l_destination_exists" "$?"

	if [ "$l_destination_exists" -eq 1 ]; then
		# dataset exists
		# Keep destination-side snapshot listing serial here. The older parallel
		# variant added complexity and was not a net win once metadata was cached.
		if zxfer_command_display_render_enabled; then
			l_write_destination_snapshot_list_to_files_cmd=$(zxfer_render_destination_zfs_command list -Hr -o name,guid -t snapshot "$l_write_destination_snapshot_list_to_files_destination_dataset")
			zxfer_echoV "Running command: $l_write_destination_snapshot_list_to_files_cmd"
			zxfer_record_last_command_string "$l_write_destination_snapshot_list_to_files_cmd"
		else
			zxfer_record_last_command_opaque
		fi
		# Run through the argv-safe destination executor and capture the
		# contents in a file in case
		# the command uses ssh
		zxfer_run_destination_zfs_cmd list -Hr -o name,guid -t snapshot "$l_write_destination_snapshot_list_to_files_destination_dataset" >"$l_rzfs_list_hr_snap_tmp_file" ||
			zxfer_throw_error "Failed to retrieve snapshot list from the destination." "$?"

	else
		# dataset does not exist
		zxfer_echoV "Destination dataset does not exist: $l_write_destination_snapshot_list_to_files_destination_dataset"
		zxfer_write_runtime_artifact_file "$l_rzfs_list_hr_snap_tmp_file" "" ||
			zxfer_throw_error "Failed to stage empty destination snapshot list." "$?"
	fi

	zxfer_normalize_destination_snapshot_list "$l_write_destination_snapshot_list_to_files_destination_dataset" "$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" ||
		return "$?"
}

# Purpose: Launch destination snapshot normalization and canonical sort into a
# caller-provided output path.
# Usage: Called by the recursive no-op proof after the matching source producer
# is started, so the identity-sorted streams can be compared directly.
# Side effects: Publishes the background helper pid in g_last_background_pid
# and writes compact status sidecars supplied by the caller.
zxfer_start_destination_snapshot_name_sorted_fifo_producer() {
	l_destination_fifo=$1
	l_dest_snapshot_err_file=$2
	l_dest_snapshot_status_file=$3
	l_dest_normalize_status_file=$4
	l_dest_stream_status_file=$5

	l_destination_dataset=$(zxfer_get_destination_snapshot_root_dataset)
	if zxfer_command_display_render_enabled; then
		l_cmd=$(zxfer_render_destination_zfs_command list -Hr -o name,guid -t snapshot "$l_destination_dataset")
		zxfer_echoV "Running command in the background: $l_cmd"
		zxfer_record_last_command_string "$l_cmd | $(zxfer_render_command_for_report "" "zxfer_normalize_destination_snapshot_stream_for_noop_proof" "$l_destination_dataset") | LC_ALL=C sort > $(zxfer_quote_token_for_report "$l_destination_fifo")"
	else
		zxfer_record_last_command_opaque
	fi

	(
		{
			zxfer_run_destination_zfs_cmd list -Hr -o name,guid -t snapshot "$l_destination_dataset" 2>"$l_dest_snapshot_err_file"
			printf '%s\n' "$?" >"$l_dest_snapshot_status_file" 2>/dev/null || :
		} | {
			zxfer_normalize_destination_snapshot_stream_for_noop_proof "$l_destination_dataset"
			l_normalize_status=$?
			printf '%s\n' "$l_normalize_status" >"$l_dest_normalize_status_file" 2>/dev/null || :
			exit "$l_normalize_status"
		} | LC_ALL=C sort >"$l_destination_fifo"
		printf '%s\n' "$?" >"$l_dest_stream_status_file" 2>/dev/null || :
	) &
	# shellcheck disable=SC2034
	zxfer_set_last_background_pid "$!"
	if ! zxfer_register_cleanup_pid \
		"$g_last_background_pid" "background destination snapshot no-op proof helper"; then
		zxfer_abort_fast_noop_background_pid \
			"$g_last_background_pid" \
			"background destination snapshot no-op proof helper" || return "$?"
		wait "$g_last_background_pid" 2>/dev/null || :
		zxfer_unregister_cleanup_pid "$g_last_background_pid"
		zxfer_clear_last_background_pid
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

	case "$l_fast_noop_abort_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac
	if zxfer_find_cleanup_pid_record "$l_fast_noop_abort_pid"; then
		zxfer_abort_cleanup_pid "$l_fast_noop_abort_pid" TERM
		return "$?"
	fi
	zxfer_abort_direct_child_pid \
		"$l_fast_noop_abort_pid" TERM "$l_fast_noop_abort_purpose"
}
