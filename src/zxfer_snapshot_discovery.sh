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
# SNAPSHOT DISCOVERY / NORMALIZATION / DELTA PREP
################################################################################

# Module contract:
# owns globals: recursive snapshot-discovery state, current-shell staged-file and recursive dataset-list scratch, destination-discovery batch scratch, and metadata-compression state.
# reads globals: g_option_j_jobs, g_option_O_origin_host, g_option_T_target_host, g_option_z_compress, g_LZFS/g_RZFS, and remote helper paths.
# mutates caches: destination-existence and snapshot-record indexes through shared helpers.
# returns via stdout: rendered discovery commands, normalized snapshot lists, and diffed dataset streams.

# Purpose: Clean up the snapshot record cache files that this module created or
# tracks.
# Usage: Called during source and destination snapshot discovery on success and
# failure paths so temporary state does not linger.
zxfer_cleanup_snapshot_record_cache_files() {
	if [ -n "${g_zxfer_source_snapshot_record_cache_file:-}" ]; then
		zxfer_cleanup_runtime_artifact_path "$g_zxfer_source_snapshot_record_cache_file"
	fi
	if [ -n "${g_zxfer_destination_snapshot_record_cache_file:-}" ]; then
		zxfer_cleanup_runtime_artifact_path "$g_zxfer_destination_snapshot_record_cache_file"
	fi
	if [ -n "${g_source_snapshot_list_sorted_file:-}" ]; then
		zxfer_cleanup_runtime_artifact_path "$g_source_snapshot_list_sorted_file"
	fi

	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
	g_source_snapshot_list_sorted_file=""
}

# Purpose: Reset the snapshot discovery state so the next snapshot-discovery
# pass starts from a clean state.
# Usage: Called during source and destination snapshot discovery before this
# module reuses mutable scratch globals or cached decisions.
zxfer_reset_snapshot_discovery_state() {
	zxfer_cleanup_snapshot_record_cache_files
	g_source_snapshot_list_cmd=""
	g_source_snapshot_list_pid=""
	g_source_snapshot_list_job_id=""
	g_source_snapshot_list_uses_parallel=0
	g_source_snapshot_list_uses_metadata_compression=0
	g_source_snapshot_list_background_sort_requested=0
	g_source_snapshot_list_sorted_file=""
	g_source_snapshot_fast_noop_attempted=0
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_recursive_dest_list=""
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_snapshot_discovery_file_read_result=""
	g_zxfer_snapshot_discovery_status_file_result=""
	g_zxfer_parallel_source_job_check_result=""
	g_zxfer_parallel_source_job_check_kind=""
	g_zxfer_recursive_dataset_list_result=""
	g_zxfer_destination_discovery_batch_inventory_status=""
	g_zxfer_destination_discovery_batch_pool_status=""
	g_zxfer_destination_discovery_batch_snapshot_status=""
	g_zxfer_destination_discovery_batch_snapshot_ran=""
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
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

# Purpose: Publish the nonblank recursive dataset list from the latest checked
# snapshot-discovery readback.
# Usage: Called after staged recursive dataset captures and exclude filters so
# blank-line handling stays identical across file-backed paths.
zxfer_publish_recursive_dataset_list_from_snapshot_discovery_read_result() {
	g_zxfer_recursive_dataset_list_result=""
	while IFS= read -r l_dataset || [ -n "$l_dataset" ]; do
		[ -n "$l_dataset" ] || continue
		if [ -n "$g_zxfer_recursive_dataset_list_result" ]; then
			g_zxfer_recursive_dataset_list_result=$g_zxfer_recursive_dataset_list_result'
'$l_dataset
		else
			g_zxfer_recursive_dataset_list_result=$l_dataset
		fi
	done <<EOF
$g_zxfer_snapshot_discovery_file_read_result
EOF
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

# Purpose: Decide whether recursive discovery may use the identity-aware
# no-op proof before the full creation-order source listing. Local and
# remote-origin (-O) sources are both eligible; -T target-host runs are not.
# Usage: Called by snapshot discovery before launching the heavier source
# discovery path.
zxfer_fast_recursive_noop_discovery_is_eligible() {
	[ "${g_option_T_target_host:-}" = "" ] || return 1
	[ "${g_option_R_recursive:-}" != "" ] || return 1
	[ "${g_option_s_make_snapshot:-0}" -eq 0 ] || return 1
	[ "${g_option_m_migrate:-0}" -eq 0 ] || return 1
	[ "${g_option_P_transfer_property:-0}" -eq 0 ] || return 1
	[ -z "${g_option_o_override_property:-}" ] || return 1
	[ "${g_option_e_restore_property_mode:-0}" -eq 0 ] || return 1
	[ "${g_option_k_backup_property_mode:-0}" -eq 0 ] || return 1

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
	g_last_background_pid=$!
	if ! zxfer_register_cleanup_pid "$g_last_background_pid" "background source snapshot no-op proof helper"; then
		if kill -s 0 "$g_last_background_pid" 2>/dev/null; then
			if ! zxfer_abort_direct_child_pid "$g_last_background_pid" TERM "background source snapshot no-op proof helper"; then
				g_last_background_pid=""
				return 1
			fi
			wait "$g_last_background_pid" 2>/dev/null || :
		fi
		g_last_background_pid=""
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
	l_cmd=$1
	l_output_file=$2
	l_error_file=${3:-}
	l_sorted_output_file=$4

	if [ -z "$l_sorted_output_file" ]; then
		zxfer_execute_background_cmd "$l_cmd" "$l_output_file" "$l_error_file"
		return "$?"
	fi

	l_cleanup_wrapper_script=$(zxfer_get_cleanup_child_wrapper_script_path) || return 1
	l_output_file_safe=$(zxfer_build_shell_command_from_argv "$l_output_file") ||
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
	l_managed_cmd="{ ( $l_cmd ); printf '%s\n' \"\$?\" > $l_source_status_file_safe; } | { tee $l_output_file_safe; printf '%s\n' \"\$?\" > $l_tee_status_file_safe; } | LC_ALL=C sort > $l_sorted_output_file_safe; l_sort_status=\$?; l_source_status=1; l_tee_status=1; if [ -f $l_source_status_file_safe ]; then IFS= read -r l_source_status < $l_source_status_file_safe || l_source_status=1; fi; if [ -f $l_tee_status_file_safe ]; then IFS= read -r l_tee_status < $l_tee_status_file_safe || l_tee_status=1; fi; rm -f $l_source_status_file_safe $l_tee_status_file_safe; case \"\$l_source_status:\$l_tee_status:\$l_sort_status\" in *[!0-9:]*) exit 1 ;; esac; [ \"\$l_source_status\" -eq 0 ] || exit \"\$l_source_status\"; [ \"\$l_tee_status\" -eq 0 ] || exit \"\$l_tee_status\"; exit \"\$l_sort_status\""

	zxfer_echoV "Executing command in the background: $l_managed_cmd"
	zxfer_record_last_command_string "$l_managed_cmd"
	if [ -n "$l_error_file" ]; then
		"$l_cleanup_wrapper_script" "$l_managed_cmd" >/dev/null 2>"$l_error_file" &
	else
		"$l_cleanup_wrapper_script" "$l_managed_cmd" >/dev/null &
	fi
	# shellcheck disable=SC2034
	g_last_background_pid=$!
	if ! zxfer_register_cleanup_pid "$g_last_background_pid" "background source snapshot discovery helper"; then
		if kill -s 0 "$g_last_background_pid" 2>/dev/null; then
			if ! zxfer_abort_direct_child_pid "$g_last_background_pid" TERM "background source snapshot discovery helper"; then
				g_last_background_pid=""
				return 1
			fi
			wait "$g_last_background_pid" 2>/dev/null || :
		fi
		g_last_background_pid=""
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
		l_cmd=$(zxfer_render_zfs_command_for_spec "$g_LZFS" list -Hr -o name,guid -s creation -t snapshot "$g_initial_source") ||
			zxfer_throw_error "${l_cmd:-Failed to render dry-run source snapshot discovery command.}" "$?"
		g_source_snapshot_list_cmd=$l_cmd
		zxfer_echoV "Dry run: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
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
	l_status=0
	zxfer_build_source_snapshot_list_cmd >"$l_cmd_tmp_file" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_read_source_snapshot_discovery_command_file "$l_cmd_tmp_file" || {
			l_read_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_cmd_tmp_file"
			zxfer_throw_error "Failed to read staged source snapshot discovery command after build failure." "$l_read_status"
		}
		l_cmd=$g_zxfer_snapshot_discovery_file_read_result
		zxfer_cleanup_runtime_artifact_path "$l_cmd_tmp_file"
		zxfer_throw_error "${l_cmd:-Failed to build source snapshot discovery command.}" "$l_status"
	fi
	zxfer_read_source_snapshot_discovery_command_file "$l_cmd_tmp_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_cmd_tmp_file"
		zxfer_throw_error "Failed to read staged source snapshot discovery command." "$l_status"
	}
	l_cmd=$g_zxfer_snapshot_discovery_file_read_result
	zxfer_cleanup_runtime_artifact_path "$l_cmd_tmp_file"
	[ -n "$l_cmd" ] || zxfer_throw_error "Staged source snapshot discovery command was empty."
	g_source_snapshot_list_cmd=$l_cmd
	if [ "$g_option_O_origin_host" != "" ]; then
		zxfer_profile_record_ssh_invocation "$g_option_O_origin_host" source
	fi

	if [ "${g_source_snapshot_list_uses_parallel:-0}" -eq 1 ]; then
		zxfer_profile_increment_counter g_zxfer_profile_source_snapshot_list_parallel_commands
	fi
	zxfer_echoV "Running command in the background: $l_cmd"
	zxfer_record_last_command_string "$l_cmd"
	if [ "${g_source_snapshot_list_background_sort_requested:-0}" -eq 1 ]; then
		zxfer_get_temp_file >/dev/null || return "$?"
		l_sorted_outfile=$g_zxfer_temp_file_result
		g_source_snapshot_list_sorted_file=$l_sorted_outfile
		if zxfer_execute_source_snapshot_list_background_cmd_with_sort \
			"$l_cmd" "$l_outfile" "$l_errfile" "$l_sorted_outfile"; then
			:
		else
			l_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_sorted_outfile"
			g_source_snapshot_list_sorted_file=""
			return "$l_status"
		fi
	else
		zxfer_execute_background_cmd "$l_cmd" "$l_outfile" "$l_errfile" || return "$?"
	fi
	g_source_snapshot_list_pid=$g_last_background_pid
	g_source_snapshot_list_job_id=""
}

# Purpose: Normalize the destination snapshot list into the stable form used
# across zxfer.
# Usage: Called during source and destination snapshot discovery before
# comparison, caching, or reporting depends on exact formatting.
#
# Normalize the destination snapshot list so it can be directly compared to the
# source listing via comm. When the user provided a trailing slash on the
# source, the destination dataset already aligns and only needs stable sorting.
zxfer_normalize_destination_snapshot_list() {
	l_destination_dataset=$1
	l_input_file=$2
	l_output_file=$3

	if [ "$g_initial_source_had_trailing_slash" -eq 1 ]; then
		if zxfer_command_display_render_enabled; then
			l_cmd="$(zxfer_render_command_for_report "LC_ALL=C" sort "$l_input_file") > $(zxfer_quote_token_for_report "$l_output_file")"
			zxfer_echoV "Running command: $l_cmd"
			zxfer_record_last_command_string "$l_cmd"
		else
			zxfer_record_last_command_opaque
		fi
		LC_ALL=C sort "$l_input_file" >"$l_output_file"
	else
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
	fi
}

# Purpose: Normalize destination snapshots from stdin for the fast no-op proof.
# Usage: Called while streaming the destination snapshot list into the
# canonical byte-order sort used by the proof, avoiding a raw full-list staging
# file that the proof cannot reuse.
zxfer_normalize_destination_snapshot_stream_for_noop_proof() {
	l_destination_dataset=$1

	if [ "$g_initial_source_had_trailing_slash" -eq 1 ]; then
		if [ -z "${g_option_x_exclude_datasets:-}" ]; then
			"${g_cmd_cat:-cat}"
			return "$?"
		fi
		l_filter_program=$(zxfer_get_snapshot_exclude_filter_awk_program)
		"${g_cmd_awk:-awk}" \
			-v "exclude_pattern=$g_option_x_exclude_datasets" \
			"$l_filter_program"
		return "$?"
	fi

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

# Purpose: Return whether a status value from the destination discovery batch
# is numeric.
# Usage: Called while parsing target-side destination discovery output before
# zxfer trusts a remote command status for local failure handling.
zxfer_destination_discovery_batch_status_is_numeric() {
	case "${1:-}" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	return 0
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

# Purpose: Reset the remote destination discovery batch scratch state.
# Usage: Called before parsing a target-side batch payload so stale statuses
# cannot leak into the current discovery result.
zxfer_reset_destination_discovery_batch_state() {
	g_zxfer_destination_discovery_batch_inventory_status=""
	g_zxfer_destination_discovery_batch_pool_status=""
	g_zxfer_destination_discovery_batch_snapshot_status=""
	g_zxfer_destination_discovery_batch_snapshot_ran=""
}

# Purpose: Build the target-side destination discovery script for the next
# remote batch execution.
# Usage: Called by the remote destination discovery path so dataset inventory,
# missing-root pool probing, and snapshot listing share one SSH round trip while
# keeping the same portable ZFS command shapes.
# Returns: A POSIX sh script suitable for `sh -c` on the target host.
zxfer_build_remote_destination_discovery_batch_script() {
	l_destination_root_dataset=$1
	l_destination_snapshot_dataset=$2
	l_destination_pool=$3
	l_target_zfs_cmd=${g_target_cmd_zfs:-$g_cmd_zfs}

	l_destination_root_dataset_single=$(zxfer_escape_for_single_quotes "$l_destination_root_dataset")
	l_destination_snapshot_dataset_single=$(zxfer_escape_for_single_quotes "$l_destination_snapshot_dataset")
	l_destination_pool_single=$(zxfer_escape_for_single_quotes "$l_destination_pool")
	l_target_zfs_cmd_single=$(zxfer_escape_for_single_quotes "$l_target_zfs_cmd")
	l_dependency_path=$(zxfer_get_effective_dependency_path)
	l_dependency_path_single=$(zxfer_escape_for_single_quotes "$l_dependency_path")

	cat <<-EOF
		PATH='$l_dependency_path_single'
		export PATH

		l_zfs_cmd='$l_target_zfs_cmd_single'
		l_destination_root_dataset='$l_destination_root_dataset_single'
		l_destination_snapshot_dataset='$l_destination_snapshot_dataset_single'
		l_destination_pool='$l_destination_pool_single'

		zxfer_cleanup_destination_discovery_batch() {
			if [ "\$l_inventory_pid" != "" ]; then
				kill "\$l_inventory_pid" 2>/dev/null || :
				wait "\$l_inventory_pid" 2>/dev/null || :
			fi
			for l_cleanup_file in "\$l_inventory_stdout_file" "\$l_inventory_stderr_file" "\$l_pool_stderr_file" "\$l_snapshot_stderr_file"; do
				[ "\$l_cleanup_file" != "" ] || continue
				rm -f "\$l_cleanup_file" 2>/dev/null || :
			done
		}

		zxfer_emit_destination_discovery_section_file() {
			l_section_name=\$1
			l_section_file=\$2

			printf '%s\t%s\n' 'BEGIN' "\$l_section_name"
			if [ -f "\$l_section_file" ]; then
				cat "\$l_section_file" || return \$?
			fi
			printf '%s\t%s\n' 'END' "\$l_section_name"
		}

		zxfer_destination_discovery_stderr_reports_missing() {
			l_stderr_file=\$1
			grep -F \
				-e 'dataset does not exist' \
				-e 'Dataset does not exist' \
				-e 'no such dataset' \
				-e 'No such dataset' \
				-e 'no such pool or dataset' \
				-e 'No such pool or dataset' \
				"\$l_stderr_file" >/dev/null 2>&1
		}

		l_inventory_stdout_file=''
		l_inventory_stderr_file=''
		l_pool_stderr_file=''
		l_snapshot_stderr_file=''
		l_inventory_pid=''
		trap 'zxfer_cleanup_destination_discovery_batch' 0
		trap 'zxfer_cleanup_destination_discovery_batch; exit 1' HUP INT TERM QUIT

		l_tmpdir=\${TMPDIR:-/tmp}
		case "\$l_tmpdir" in
		/*)
			:
			;;
		*)
			l_tmpdir=/tmp
			;;
		esac
		umask 077
		l_inventory_stdout_file=\$(mktemp "\$l_tmpdir/zxfer.destination-discovery.inventory.XXXXXX" 2>/dev/null) || exit \$?
		l_inventory_stderr_file=\$(mktemp "\$l_tmpdir/zxfer.destination-discovery.inventory-stderr.XXXXXX" 2>/dev/null) || exit \$?
		l_pool_stderr_file=\$(mktemp "\$l_tmpdir/zxfer.destination-discovery.pool-stderr.XXXXXX" 2>/dev/null) || exit \$?
		l_snapshot_stderr_file=\$(mktemp "\$l_tmpdir/zxfer.destination-discovery.snapshots-stderr.XXXXXX" 2>/dev/null) || exit \$?

		"\$l_zfs_cmd" list -t filesystem,volume -Hr -o name "\$l_destination_root_dataset" >"\$l_inventory_stdout_file" 2>"\$l_inventory_stderr_file" &
		l_inventory_pid=\$!
		l_pool_status=''
		l_snapshot_status=0
		l_snapshot_ran=1

		printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
		printf '%s\t%s\n' 'BEGIN' 'snapshot_stdout'
		"\$l_zfs_cmd" list -Hr -o name,guid -t snapshot "\$l_destination_snapshot_dataset" 2>"\$l_snapshot_stderr_file"
		l_snapshot_status=\$?
		printf '%s\t%s\n' 'END' 'snapshot_stdout'

		l_inventory_status=0
		wait "\$l_inventory_pid" || l_inventory_status=\$?
		l_inventory_pid=''

		if [ "\$l_inventory_status" -ne 0 ]; then
			if zxfer_destination_discovery_stderr_reports_missing "\$l_inventory_stderr_file"; then
				"\$l_zfs_cmd" list -H -o name "\$l_destination_pool" >/dev/null 2>"\$l_pool_stderr_file"
				l_pool_status=\$?
				if [ "\$l_pool_status" -eq 0 ] && zxfer_destination_discovery_stderr_reports_missing "\$l_snapshot_stderr_file"; then
					l_snapshot_status=0
					: >"\$l_snapshot_stderr_file"
				fi
			fi
		fi

		if [ "\$l_inventory_status" -eq 0 ]; then
			grep -F -x -e "\$l_destination_snapshot_dataset" "\$l_inventory_stdout_file" >/dev/null 2>&1
			l_grep_status=\$?
			case "\$l_grep_status" in
			0)
				:
				;;
			1)
				l_snapshot_status=0
				: >"\$l_snapshot_stderr_file"
				:
				;;
			*)
				l_inventory_status=\$l_grep_status
				printf 'Failed to scan destination dataset inventory for %s.\n' "\$l_destination_snapshot_dataset" >"\$l_inventory_stderr_file"
				;;
			esac
		fi

		printf '%s\t%s\t%s\n' 'STATUS' 'inventory' "\$l_inventory_status"
		printf '%s\t%s\t%s\n' 'STATUS' 'pool' "\$l_pool_status"
		printf '%s\t%s\t%s\n' 'STATUS' 'snapshot_ran' "\$l_snapshot_ran"
		zxfer_emit_destination_discovery_section_file inventory_stdout "\$l_inventory_stdout_file" || exit \$?
		zxfer_emit_destination_discovery_section_file inventory_stderr "\$l_inventory_stderr_file" || exit \$?
		zxfer_emit_destination_discovery_section_file pool_stderr "\$l_pool_stderr_file" || exit \$?
		printf '%s\t%s\t%s\n' 'STATUS' 'snapshot' "\$l_snapshot_status"
		zxfer_emit_destination_discovery_section_file snapshot_stderr "\$l_snapshot_stderr_file" || exit \$?
		printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
	EOF
}

# Purpose: Load the compact status sidecar from destination discovery parsing.
# Usage: Called after the batch output file has been split into staged payload
# files without replaying large snapshot lists through a shell loop.
zxfer_load_destination_discovery_batch_status_file() {
	l_status_file=$1
	l_tab='	'

	zxfer_reset_destination_discovery_batch_state

	l_seen_inventory_status=0
	l_seen_pool_status=0
	l_seen_snapshot_status=0
	l_seen_snapshot_ran_status=0

	while IFS= read -r l_status_line || [ -n "$l_status_line" ]; do
		case "$l_status_line" in
		*"$l_tab"*)
			l_status_name=${l_status_line%%"$l_tab"*}
			l_status_value=${l_status_line#*"$l_tab"}
			;;
		*)
			return 1
			;;
		esac
		case "$l_status_name" in
		inventory)
			[ "$l_seen_inventory_status" -eq 0 ] || return 1
			g_zxfer_destination_discovery_batch_inventory_status=$l_status_value
			l_seen_inventory_status=1
			;;
		pool)
			[ "$l_seen_pool_status" -eq 0 ] || return 1
			g_zxfer_destination_discovery_batch_pool_status=$l_status_value
			l_seen_pool_status=1
			;;
		snapshot)
			[ "$l_seen_snapshot_status" -eq 0 ] || return 1
			g_zxfer_destination_discovery_batch_snapshot_status=$l_status_value
			l_seen_snapshot_status=1
			;;
		snapshot_ran)
			[ "$l_seen_snapshot_ran_status" -eq 0 ] || return 1
			g_zxfer_destination_discovery_batch_snapshot_ran=$l_status_value
			l_seen_snapshot_ran_status=1
			;;
		*)
			return 1
			;;
		esac
	done <"$l_status_file"

	[ "$l_seen_inventory_status" -eq 1 ] || return 1
	[ "$l_seen_pool_status" -eq 1 ] || return 1
	[ "$l_seen_snapshot_status" -eq 1 ] || return 1
	[ "$l_seen_snapshot_ran_status" -eq 1 ] || return 1
	zxfer_destination_discovery_batch_status_is_numeric "$g_zxfer_destination_discovery_batch_inventory_status" || return 1
	zxfer_destination_discovery_batch_status_is_numeric "$g_zxfer_destination_discovery_batch_snapshot_status" || return 1
	zxfer_destination_discovery_batch_status_is_numeric "$g_zxfer_destination_discovery_batch_snapshot_ran" || return 1
	if [ -n "$g_zxfer_destination_discovery_batch_pool_status" ]; then
		zxfer_destination_discovery_batch_status_is_numeric "$g_zxfer_destination_discovery_batch_pool_status" || return 1
	fi
}

# Purpose: Split target-side destination discovery output into staged files and
# a compact status sidecar.
# Usage: Called with batch payload on stdin so large snapshot sections can be
# streamed through awk into final staging files instead of captured wholesale.
zxfer_split_remote_destination_discovery_batch_stream_to_files() {
	l_batch_status_file=$1
	l_dest_list_tmp_file=$2
	l_dest_list_err_file=$3
	l_rzfs_list_hr_snap_tmp_file=$4
	l_rzfs_list_hr_snap_err_tmp_file=$5

	zxfer_write_runtime_artifact_file "$l_dest_list_tmp_file" "" || return "$?"
	zxfer_write_runtime_artifact_file "$l_dest_list_err_file" "" || return "$?"
	zxfer_write_runtime_artifact_file "$l_rzfs_list_hr_snap_tmp_file" "" || return "$?"
	zxfer_write_runtime_artifact_file "$l_rzfs_list_hr_snap_err_tmp_file" "" || return "$?"
	zxfer_write_runtime_artifact_file "$l_batch_status_file" "" || return "$?"

	# shellcheck disable=SC2016  # awk program should see literal $0.
	"${g_cmd_awk:-awk}" \
		-v dest_out="$l_dest_list_tmp_file" \
		-v dest_err="$l_dest_list_err_file" \
		-v snap_out="$l_rzfs_list_hr_snap_tmp_file" \
		-v snap_err="$l_rzfs_list_hr_snap_err_tmp_file" \
		-v status_out="$l_batch_status_file" '
		function fail() {
			bad = 1
		}
		function record_status(name, value) {
			if (name == "inventory") {
				if (seen_inventory_status != 0) {
					fail()
				}
				inventory_status = value
				seen_inventory_status = 1
			} else if (name == "pool") {
				if (seen_pool_status != 0) {
					fail()
				}
				pool_status = value
				seen_pool_status = 1
			} else if (name == "snapshot") {
				if (seen_snapshot_status != 0) {
					fail()
				}
				snapshot_status = value
				seen_snapshot_status = 1
			} else if (name == "snapshot_ran") {
				if (seen_snapshot_ran_status != 0) {
					fail()
				}
				snapshot_ran_status = value
				seen_snapshot_ran_status = 1
			} else {
				fail()
			}
		}
		function begin_section(name) {
			if (current_section != "") {
				fail()
			}
			if (name == "inventory_stdout") {
				if (seen_inventory_stdout != 0) {
					fail()
				}
				current_output = dest_out
				seen_inventory_stdout = 1
			} else if (name == "inventory_stderr") {
				if (seen_inventory_stderr != 0) {
					fail()
				}
				current_output = dest_err
				seen_inventory_stderr = 1
			} else if (name == "pool_stderr") {
				if (seen_pool_stderr != 0) {
					fail()
				}
				current_output = ""
				seen_pool_stderr = 1
			} else if (name == "snapshot_stdout") {
				if (seen_snapshot_stdout != 0) {
					fail()
				}
				current_output = snap_out
				seen_snapshot_stdout = 1
			} else if (name == "snapshot_stderr") {
				if (seen_snapshot_stderr != 0) {
					fail()
				}
				current_output = snap_err
				seen_snapshot_stderr = 1
			} else {
				fail()
			}
			current_section = name
		}
		function append_section_line(line) {
			if (current_output != "") {
				print line >> current_output
			}
		}
		BEGIN {
			tab = sprintf("%c", 9)
			current_section = ""
			current_output = ""
		}
		{
			if (bad != 0) {
				next
			}
			if (seen_header == 0) {
				if ($0 != "ZXFER_DESTINATION_DISCOVERY_BATCH_V1") {
					fail()
				}
				seen_header = 1
				next
			}
			if ($0 == "ZXFER_DESTINATION_DISCOVERY_BATCH_END") {
				if (current_section != "") {
					fail()
				}
				seen_end = 1
				next
			}
			if (seen_end != 0) {
				if ($0 != "") {
					fail()
				}
				next
			}
			if (current_section != "") {
				if (index($0, "END" tab) == 1) {
					section_name = substr($0, 5)
					if (section_name == current_section) {
						current_section = ""
						current_output = ""
						next
					}
				}
				append_section_line($0)
				next
			}
			if (index($0, "STATUS" tab) == 1) {
				status_record = substr($0, 8)
				status_separator = index(status_record, tab)
				if (status_separator == 0) {
					fail()
				}
				record_status(substr(status_record, 1, status_separator - 1), substr(status_record, status_separator + 1))
				next
			}
			if (index($0, "BEGIN" tab) == 1) {
				begin_section(substr($0, 7))
				next
			}
			if (index($0, "END" tab) == 1) {
				fail()
				next
			}
			fail()
		}
		END {
			if (bad != 0) {
				exit 1
			}
			if (seen_header != 1 || seen_end != 1 || current_section != "") {
				exit 1
			}
			if (seen_inventory_status != 1 || seen_pool_status != 1 || seen_snapshot_status != 1 || seen_snapshot_ran_status != 1) {
				exit 1
			}
			if (seen_inventory_stdout != 1 || seen_inventory_stderr != 1 || seen_pool_stderr != 1 || seen_snapshot_stdout != 1 || seen_snapshot_stderr != 1) {
				exit 1
			}
			print "inventory" tab inventory_status > status_out
			print "pool" tab pool_status > status_out
			print "snapshot" tab snapshot_status > status_out
			print "snapshot_ran" tab snapshot_ran_status > status_out
			close(status_out)
			close(dest_out)
			close(dest_err)
			close(snap_out)
			close(snap_err)
		}
	'
}

# Purpose: Run target-side destination discovery through one remote SSH shell
# invocation and stage its results.
# Usage: Called by snapshot discovery when `-T` is active to avoid separate
# target SSH round trips for destination dataset inventory and snapshot listing.
zxfer_run_remote_destination_discovery_batch_to_files() {
	l_destination_dataset=$1
	l_dest_list_tmp_file=$2
	l_dest_list_err_file=$3
	l_rzfs_list_hr_snap_tmp_file=$4
	l_rzfs_list_hr_snap_err_tmp_file=$5
	l_destination_pool=${g_destination%%/*}
	l_transport_status_file=""
	l_transport_stderr_file=""
	l_batch_status_file=""

	zxfer_reset_destination_discovery_batch_state

	l_remote_script=$(zxfer_build_remote_destination_discovery_batch_script \
		"$g_destination" "$l_destination_dataset" "$l_destination_pool") ||
		return "$?"
	l_remote_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_script") ||
		return "$?"
	l_transport_tokens=$(zxfer_get_ssh_transport_tokens_for_host "$g_option_T_target_host") ||
		zxfer_throw_error "$l_transport_tokens" "$?"
	# Prevalidate wrapper-style host specs outside the streaming pipeline so
	# setup failures still exit through the parent shell's reporting path.
	if zxfer_prepare_ssh_shell_command_context "$g_option_T_target_host" "$l_remote_cmd"; then
		:
	else
		l_status=$?
		if [ "$g_zxfer_ssh_shell_context_error_result" != "" ]; then
			zxfer_throw_error "$g_zxfer_ssh_shell_context_error_result"
		fi
		return "$l_status"
	fi

	zxfer_get_temp_file >/dev/null || return "$?"
	l_transport_status_file=$g_zxfer_temp_file_result
	zxfer_get_temp_file >/dev/null || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_transport_status_file"
		return "$l_status"
	}
	l_transport_stderr_file=$g_zxfer_temp_file_result
	zxfer_get_temp_file >/dev/null || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file"
		return "$l_status"
	}
	l_batch_status_file=$g_zxfer_temp_file_result

	zxfer_echoV "Running remote destination discovery batch for $g_destination."
	l_parse_status=0
	{
		l_transport_status=0
		zxfer_invoke_ssh_shell_command_for_host "$g_option_T_target_host" "$l_remote_cmd" destination 2>"$l_transport_stderr_file" ||
			l_transport_status=$?
		printf '%s\n' "$l_transport_status" >"$l_transport_status_file" || :
	} | zxfer_split_remote_destination_discovery_batch_stream_to_files \
		"$l_batch_status_file" \
		"$l_dest_list_tmp_file" \
		"$l_dest_list_err_file" \
		"$l_rzfs_list_hr_snap_tmp_file" \
		"$l_rzfs_list_hr_snap_err_tmp_file" || l_parse_status=$?

	zxfer_read_snapshot_discovery_capture_file "$l_transport_status_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file" "$l_batch_status_file"
		return "$l_status"
	}
	l_batch_status=$g_zxfer_snapshot_discovery_file_read_result
	case "$l_batch_status" in
	*'
')
		l_batch_status=${l_batch_status%?}
		;;
	esac
	case "$l_batch_status" in
	'' | *[!0-9]*)
		zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file" "$l_batch_status_file"
		zxfer_throw_error "Malformed destination discovery transport status."
		;;
	esac
	if [ "$l_batch_status" -ne 0 ]; then
		zxfer_read_snapshot_discovery_capture_file "$l_transport_stderr_file" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file" "$l_batch_status_file"
			return "$l_status"
		}
		l_transport_stderr=$g_zxfer_snapshot_discovery_file_read_result
		l_status=0
		zxfer_write_runtime_artifact_file "$l_dest_list_err_file" "$l_transport_stderr" || l_status=$?
		zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file" "$l_batch_status_file"
		if [ "$l_status" -ne 0 ]; then
			return "$l_status"
		fi
		return "$l_batch_status"
	fi

	if [ "$l_parse_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file" "$l_batch_status_file"
		zxfer_throw_error "Malformed destination discovery batch response." "$l_parse_status"
	fi

	l_status=0
	zxfer_load_destination_discovery_batch_status_file "$l_batch_status_file" || l_status=$?
	zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file" "$l_batch_status_file"
	if [ "$l_status" -ne 0 ]; then
		zxfer_throw_error "Malformed destination discovery batch response." "$l_status"
	fi
	zxfer_profile_record_zfs_call destination list
	if [ -n "${g_zxfer_destination_discovery_batch_pool_status:-}" ]; then
		zxfer_profile_record_zfs_call destination list
	fi
	if [ "${g_zxfer_destination_discovery_batch_snapshot_ran:-0}" -eq 1 ]; then
		zxfer_profile_record_zfs_call destination list
	fi
}

# Purpose: Publish destination dataset inventory from staged files into the
# shared discovery state.
# Usage: Called by local and remote destination discovery after inventory
# commands have completed so missing-root bootstrap and failure handling stay
# identical.
zxfer_publish_destination_dataset_inventory_from_stage() {
	l_dest_list_tmp_file=$1
	l_dest_list_err_file=$2
	l_dest_inventory_status=$3
	l_dest_pool_status=${4:-}

	if [ "$l_dest_inventory_status" -eq 0 ]; then
		zxfer_read_snapshot_discovery_capture_file "$l_dest_list_tmp_file" ||
			zxfer_throw_error "Failed to read staged destination dataset inventory." "$?"
		g_recursive_dest_list=$g_zxfer_snapshot_discovery_file_read_result
		[ -n "$g_recursive_dest_list" ] || {
			zxfer_throw_error "Staged destination dataset inventory was empty."
		}
		zxfer_seed_destination_existence_cache_from_recursive_list "$g_destination" "$g_recursive_dest_list"
		return
	fi

	zxfer_read_snapshot_discovery_capture_file "$l_dest_list_err_file" ||
		zxfer_throw_error "Failed to read staged destination dataset inventory stderr." "$?"
	l_dest_err=$g_zxfer_snapshot_discovery_file_read_result
	if zxfer_destination_probe_reports_missing "$l_dest_err"; then
		if [ -z "$l_dest_pool_status" ]; then
			l_dest_pool=${g_destination%%/*}
			l_dest_pool_status=0
			l_dest_pool_err=$(zxfer_run_destination_zfs_cmd list -H -o name "$l_dest_pool" 2>&1 >/dev/null) ||
				l_dest_pool_status=$?
		else
			l_dest_pool=${g_destination%%/*}
			l_dest_pool_err=""
		fi
		if [ "$l_dest_pool_status" -eq 0 ]; then
			g_recursive_dest_list=""
			zxfer_mark_destination_root_missing_in_cache "$g_destination"
			zxfer_echoV "Destination dataset missing; treating as empty list for bootstrap."
		else
			l_dest_pool_err=$(zxfer_limit_snapshot_discovery_capture_lines "$l_dest_pool_err" 5)
			if [ -n "$l_dest_pool_err" ]; then
				zxfer_throw_error "Destination dataset [$g_destination] is missing and destination pool [$l_dest_pool] could not be listed: $l_dest_pool_err" "$l_dest_pool_status"
			fi
			zxfer_throw_error "Destination dataset [$g_destination] is missing and destination pool [$l_dest_pool] could not be listed." "$l_dest_pool_status"
		fi
	else
		l_dest_err=$(zxfer_limit_snapshot_discovery_capture_lines "$l_dest_err" 5)
		if [ -n "$l_dest_err" ]; then
			zxfer_throw_error "Failed to retrieve list of datasets from the destination: $l_dest_err" "$l_dest_inventory_status"
		fi
		zxfer_throw_error "Failed to retrieve list of datasets from the destination" "$l_dest_inventory_status"
	fi
}

# Purpose: Collect and publish destination dataset inventory through the local
# destination execution path.
# Usage: Called after snapshot diffing when later work has proven it can
# consume the recursive destination existence cache.
zxfer_collect_local_destination_dataset_inventory() {
	zxfer_create_temp_file_group 2 >/dev/null || return "$?"
	l_destination_inventory_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_dest_list_tmp_file
		IFS= read -r l_dest_list_err_file
	} <<-EOF
		$l_destination_inventory_stage_files
	EOF

	if zxfer_command_display_render_enabled; then
		l_cmd=$(zxfer_render_destination_zfs_command list -t filesystem,volume -Hr -o name "$g_destination")
		zxfer_echoV "Running command: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
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

	l_destination_dataset=$(zxfer_get_destination_snapshot_root_dataset)

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		l_cmd=$(zxfer_render_destination_zfs_command list -Hr -o name,guid -t snapshot "$l_destination_dataset") ||
			zxfer_throw_error "${l_cmd:-Failed to render dry-run destination snapshot discovery command.}" "$?"
		zxfer_echoV "Dry run: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
		zxfer_write_runtime_artifact_file "$l_rzfs_list_hr_snap_tmp_file" "" || return "$?"
		zxfer_write_runtime_artifact_file "$l_dest_snaps_stripped_sorted_tmp_file" "" || return "$?"
		return
	fi

	# check if the destination zfs dataset exists before listing snapshots
	l_destination_exists=$(zxfer_exists_destination "$l_destination_dataset") ||
		zxfer_throw_error "$l_destination_exists" "$?"

	if [ "$l_destination_exists" -eq 1 ]; then
		# dataset exists
		# Keep destination-side snapshot listing serial here. The older parallel
		# variant added complexity and was not a net win once metadata was cached.
		if zxfer_command_display_render_enabled; then
			l_cmd=$(zxfer_render_destination_zfs_command list -Hr -o name,guid -t snapshot "$l_destination_dataset")
			zxfer_echoV "Running command: $l_cmd"
			zxfer_record_last_command_string "$l_cmd"
		else
			zxfer_record_last_command_opaque
		fi
		# make sure to eval and then pipe the contents to the file in case
		# the command uses ssh
		zxfer_run_destination_zfs_cmd list -Hr -o name,guid -t snapshot "$l_destination_dataset" >"$l_rzfs_list_hr_snap_tmp_file" ||
			zxfer_throw_error "Failed to retrieve snapshot list from the destination." "$?"

	else
		# dataset does not exist
		zxfer_echoV "Destination dataset does not exist: $l_destination_dataset"
		zxfer_write_runtime_artifact_file "$l_rzfs_list_hr_snap_tmp_file" "" ||
			zxfer_throw_error "Failed to stage empty destination snapshot list." "$?"
	fi

	zxfer_normalize_destination_snapshot_list "$l_destination_dataset" "$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" ||
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
	g_last_background_pid=$!
	if ! zxfer_register_cleanup_pid "$g_last_background_pid" "background destination snapshot no-op proof helper"; then
		zxfer_abort_fast_noop_background_pid "$g_last_background_pid" "background destination snapshot no-op proof helper"
		wait "$g_last_background_pid" 2>/dev/null || :
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

	case "$l_fast_noop_abort_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac
	if ! kill -s 0 "$l_fast_noop_abort_pid" 2>/dev/null; then
		zxfer_unregister_cleanup_pid "$l_fast_noop_abort_pid"
		return 0
	fi

	zxfer_abort_cleanup_pid "$l_fast_noop_abort_pid" >/dev/null 2>&1 ||
		zxfer_abort_direct_child_pid "$l_fast_noop_abort_pid" TERM "$l_fast_noop_abort_purpose" >/dev/null 2>&1 ||
		:
	if kill -s 0 "$l_fast_noop_abort_pid" 2>/dev/null; then
		kill "$l_fast_noop_abort_pid" 2>/dev/null || :
	fi

	return 0
}

# Purpose: Write both recursive snapshot delta directions in one sorted-list
# pass.
# Usage: Called after source and destination snapshot lists have been sorted
# and normalized, before recursive dataset work queues are derived.
zxfer_write_snapshot_delta_files() {
	l_source_sorted_file=$1
	l_destination_sorted_file=$2
	l_source_missing_file=$3
	l_destination_extra_file=$4

	zxfer_get_temp_file >/dev/null || return "$?"
	l_combined_delta_file=$g_zxfer_temp_file_result

	zxfer_write_runtime_artifact_file "$l_source_missing_file" "" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_combined_delta_file"
		return "$l_status"
	}
	zxfer_write_runtime_artifact_file "$l_destination_extra_file" "" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_combined_delta_file"
		return "$l_status"
	}

	LC_ALL=C comm -3 "$l_source_sorted_file" "$l_destination_sorted_file" >"$l_combined_delta_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_combined_delta_file"
		return "$l_status"
	}

	# `comm -3` prefixes destination-only records with one tab. Snapshot names
	# cannot begin with tabs, while GUID records may contain tabs after the
	# name, so removing exactly one leading tab preserves the serialized record.
	l_status=0
	# shellcheck disable=SC2016  # awk script should see literal $0.
	"${g_cmd_awk:-awk}" \
		-v source_file="$l_source_missing_file" \
		-v destination_file="$l_destination_extra_file" '
		substr($0, 1, 1) == "\t" {
			print substr($0, 2) >> destination_file
			next
		}
		{
			print $0 >> source_file
		}
		END {
			close(source_file)
			close(destination_file)
		}
	' "$l_combined_delta_file" ||
		l_status=$?

	zxfer_cleanup_runtime_artifact_path "$l_combined_delta_file"
	return "$l_status"
}

# Purpose: Filter sorted snapshot records by dataset using the configured
# exclude pattern.
# Usage: Called before recursive diff planning so excluded datasets cannot keep
# otherwise no-op source and destination snapshot lists from short-circuiting.
zxfer_filter_snapshot_file_with_excludes() {
	l_snapshot_input_file=$1
	l_snapshot_output_file=$2

	[ -n "${g_option_x_exclude_datasets:-}" ] || {
		cat "$l_snapshot_input_file" >"$l_snapshot_output_file"
		return "$?"
	}

	# shellcheck disable=SC2016  # awk script should see literal $0.
	"${g_cmd_awk:-awk}" \
		-v exclude_pattern="$g_option_x_exclude_datasets" '
		{
			snapshot_path = $0
			if (substr(snapshot_path, 1, 1) == "\t") {
				snapshot_path = substr(snapshot_path, 2)
			}
			tab_pos = index(snapshot_path, "\t")
			if (tab_pos > 0) {
				snapshot_path = substr(snapshot_path, 1, tab_pos - 1)
			}
		at_pos = index(snapshot_path, "@")
		snapshot_dataset = snapshot_path
		if (at_pos > 0) {
			snapshot_dataset = substr(snapshot_path, 1, at_pos - 1)
		}
		if (snapshot_dataset !~ exclude_pattern) {
			print
		}
	}' "$l_snapshot_input_file" >"$l_snapshot_output_file"
}

# Purpose: Check whether zxfer should use linear reverse for file.
# Usage: Called during source and destination snapshot discovery when later
# helpers need a boolean branch decision about the current configuration or
# live state.
#
# Reverse snapshot lists with a bounded linear awk path, but keep the older
# sort-based fallback for very large inputs so reversal does not become
# unbounded-memory work on hosts with small awk heaps.
zxfer_should_use_linear_reverse_for_file() {
	l_input_file=$1
	l_max_lines=${g_zxfer_linear_reverse_max_lines:-50000}

	case "$l_max_lines" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	[ "$l_max_lines" -gt 0 ] || return 1

	l_line_count_status=0
	l_line_count=$("${g_cmd_awk:-awk}" 'END { print NR + 0 }' "$l_input_file" 2>/dev/null) ||
		l_line_count_status=$?
	[ "$l_line_count_status" -eq 0 ] || return "$l_line_count_status"
	case "$l_line_count" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	[ "$l_line_count" -le "$l_max_lines" ]
}

# Purpose: Reverse the plain file lines with sort while preserving the record
# structure later helpers rely on.
# Usage: Called during source and destination snapshot discovery when
# comparison or replay logic needs the same data in the opposite order.
zxfer_reverse_plain_file_lines_with_sort() {
	l_input_file=$1
	zxfer_get_temp_file >/dev/null || return "$?"
	l_numbered_tmp_file=$g_zxfer_temp_file_result

	cat -n "$l_input_file" >"$l_numbered_tmp_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_numbered_tmp_file"
		return "$l_status"
	}

	l_status=0
	# Reverse by the cat -n line numbers, then strip the number column.
	LC_ALL=C sort -nr "$l_numbered_tmp_file" | cut -f2- || l_status=$?
	zxfer_cleanup_runtime_artifact_path "$l_numbered_tmp_file"
	return "$l_status"
}

# Purpose: Reverse the file lines while preserving the record structure later
# helpers rely on.
# Usage: Called during source and destination snapshot discovery when
# comparison or replay logic needs the same data in the opposite order.
zxfer_reverse_file_lines() {
	l_input_file=$1

	if zxfer_should_use_linear_reverse_for_file "$l_input_file"; then
		# shellcheck disable=SC2016  # awk program should see literal $0/NR.
		"${g_cmd_awk:-awk}" '{ l_lines[NR] = $0 } END { for (l_i = NR; l_i >= 1; l_i--) print l_lines[l_i] }' "$l_input_file"
	else
		zxfer_reverse_plain_file_lines_with_sort "$l_input_file"
	fi
}

# Purpose: Capture the recursive dataset list from lines file into staged state
# or module globals for later use.
# Usage: Called during source and destination snapshot discovery when later
# helpers need a checked snapshot of command output or computed state.
zxfer_capture_recursive_dataset_list_from_lines_file() {
	l_dataset_lines_file=$1

	g_zxfer_recursive_dataset_list_result=""
	[ -n "$l_dataset_lines_file" ] || return 0
	[ -f "$l_dataset_lines_file" ] || return 0

	zxfer_get_temp_file >/dev/null || return "$?"
	l_dataset_list_sorted_file=$g_zxfer_temp_file_result

	LC_ALL=C sort -u "$l_dataset_lines_file" >"$l_dataset_list_sorted_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dataset_list_sorted_file"
		return "$l_status"
	}

	zxfer_read_snapshot_discovery_capture_file "$l_dataset_list_sorted_file" || {
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dataset_list_sorted_file"
		return "$l_read_status"
	}

	zxfer_publish_recursive_dataset_list_from_snapshot_discovery_read_result

	zxfer_cleanup_runtime_artifact_path "$l_dataset_list_sorted_file"
	return 0
}

# Purpose: Capture the recursive dataset list from snapshot file into staged
# state or module globals for later use.
# Usage: Called during source and destination snapshot discovery when later
# helpers need a checked snapshot of command output or computed state.
zxfer_capture_recursive_dataset_list_from_snapshot_file() {
	l_snapshot_records_file=$1

	g_zxfer_recursive_dataset_list_result=""
	[ -n "$l_snapshot_records_file" ] || return 0
	[ -f "$l_snapshot_records_file" ] || return 0

	zxfer_get_temp_file >/dev/null || return "$?"
	l_dataset_lines_file=$g_zxfer_temp_file_result

	# Stage the dataset name (everything before @) of every snapshot record.
	# shellcheck disable=SC2016  # awk script should see literal $1.
	"$g_cmd_awk" -F@ '{print $1}' "$l_snapshot_records_file" >"$l_dataset_lines_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dataset_lines_file"
		return "$l_status"
	}

	zxfer_capture_recursive_dataset_list_from_lines_file "$l_dataset_lines_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dataset_lines_file"
		return "$l_status"
	}

	zxfer_cleanup_runtime_artifact_path "$l_dataset_lines_file"
	return 0
}

# Purpose: Filter the recursive dataset list with excludes down to the subset
# later helpers should act on.
# Usage: Called during source and destination snapshot discovery before
# reconciliation, execution, or reporting consumes the reduced set.
zxfer_filter_recursive_dataset_list_with_excludes() {
	l_dataset_list=$1

	g_zxfer_recursive_dataset_list_result=""
	[ -n "$l_dataset_list" ] || return 0
	[ -n "${g_option_x_exclude_datasets:-}" ] || {
		g_zxfer_recursive_dataset_list_result=$l_dataset_list
		return 0
	}

	zxfer_create_temp_file_group 2 >/dev/null || return "$?"
	l_filter_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_dataset_list_input_file
		IFS= read -r l_dataset_list_filtered_file
	} <<-EOF
		$l_filter_stage_files
	EOF

	if zxfer_write_runtime_artifact_file "$l_dataset_list_input_file" "$l_dataset_list
"; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path_list_and_return "$l_status" "$l_filter_stage_files"
		return "$?"
	fi

	l_filter_status=0
	grep -v -e "$g_option_x_exclude_datasets" "$l_dataset_list_input_file" >"$l_dataset_list_filtered_file" ||
		l_filter_status=$?
	case "$l_filter_status" in
	0 | 1) ;;
	*)
		zxfer_cleanup_runtime_artifact_path_list_and_return "$l_filter_status" "$l_filter_stage_files"
		return "$?"
		;;
	esac

	zxfer_read_snapshot_discovery_capture_file "$l_dataset_list_filtered_file" || {
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path_list_and_return "$l_read_status" "$l_filter_stage_files"
		return "$?"
	}

	zxfer_publish_recursive_dataset_list_from_snapshot_discovery_read_result

	zxfer_cleanup_runtime_artifact_path_list "$l_filter_stage_files"
	return 0
}

# Purpose: Decide whether recursive source dataset inventory must be derived
# from the full source snapshot list.
# Usage: Called during recursive diff planning so no-op runs without property
# work can avoid an otherwise unused whole-tree dataset extraction.
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

# Purpose: Update the g recursive source list in the shared runtime state.
# Usage: Called during source and destination snapshot discovery after a probe
# or planning step changes the active context that later helpers should use.
zxfer_set_g_recursive_source_list() {
	l_lzfs_list_hr_s_snap_tmp_file=$1
	l_dest_snaps_stripped_sorted_tmp_file=$2
	l_presorted_source_snaps_tmp_file=${3:-}

	zxfer_create_temp_file_group 5 >/dev/null || return "$?"
	l_delta_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_source_snaps_sorted_tmp_file
		IFS= read -r l_missing_snapshots_tmp_file
		IFS= read -r l_destination_extra_snapshots_tmp_file
		IFS= read -r l_source_snaps_filtered_tmp_file
		IFS= read -r l_destination_snaps_filtered_tmp_file
	} <<-EOF
		$l_delta_stage_files
	EOF

	if [ -n "$l_presorted_source_snaps_tmp_file" ]; then
		if [ -f "$l_presorted_source_snaps_tmp_file" ]; then
			l_source_snaps_sorted_input_file=$l_presorted_source_snaps_tmp_file
		else
			zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
			zxfer_throw_error "Failed to locate staged sorted source snapshots for recursive delta planning."
		fi
	else
		l_source_snaps_sorted_input_file=$l_source_snaps_sorted_tmp_file
		# sort the source snapshots for use with comm
		# wait until background processes are finished before attempting to sort
		if zxfer_command_display_render_enabled; then
			l_cmd="$(zxfer_render_command_for_report "LC_ALL=C" sort "$l_lzfs_list_hr_s_snap_tmp_file") > $(zxfer_quote_token_for_report "$l_source_snaps_sorted_tmp_file")"
			zxfer_echoV "Running command: $l_cmd"
			zxfer_record_last_command_string "$l_cmd"
		else
			zxfer_record_last_command_opaque
		fi
		LC_ALL=C sort "$l_lzfs_list_hr_s_snap_tmp_file" >"$l_source_snaps_sorted_tmp_file" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
			zxfer_throw_error "Failed to sort source snapshots for recursive delta planning." "$l_status"
		}
	fi

	l_source_snaps_diff_input_file=$l_source_snaps_sorted_input_file
	l_dest_snaps_diff_input_file=$l_dest_snaps_stripped_sorted_tmp_file
	if [ "$g_option_x_exclude_datasets" != "" ]; then
		zxfer_filter_snapshot_file_with_excludes \
			"$l_source_snaps_sorted_input_file" \
			"$l_source_snaps_filtered_tmp_file" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
			zxfer_throw_error "Failed to filter source snapshots against exclude patterns for recursive delta planning." "$l_status"
		}
		zxfer_filter_snapshot_file_with_excludes \
			"$l_dest_snaps_stripped_sorted_tmp_file" \
			"$l_destination_snaps_filtered_tmp_file" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
			zxfer_throw_error "Failed to filter destination snapshots against exclude patterns for recursive delta planning." "$l_status"
		}
		l_source_snaps_diff_input_file=$l_source_snaps_filtered_tmp_file
		l_dest_snaps_diff_input_file=$l_destination_snaps_filtered_tmp_file
	fi

	if cmp -s "$l_source_snaps_diff_input_file" "$l_dest_snaps_diff_input_file"; then
		zxfer_write_runtime_artifact_file "$l_missing_snapshots_tmp_file" "" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
			zxfer_throw_error "Failed to stage empty recursive source snapshot delta." "$l_status"
		}
		zxfer_write_runtime_artifact_file "$l_destination_extra_snapshots_tmp_file" "" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
			zxfer_throw_error "Failed to stage empty recursive destination snapshot delta." "$l_status"
		}
	else
		l_cmp_status=$?
		if [ "$l_cmp_status" -gt 1 ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
			zxfer_throw_error "Failed to compare source and destination snapshots for recursive delta planning." "$l_cmp_status"
		fi

		zxfer_write_snapshot_delta_files \
			"$l_source_snaps_diff_input_file" \
			"$l_dest_snaps_diff_input_file" \
			"$l_missing_snapshots_tmp_file" \
			"$l_destination_extra_snapshots_tmp_file" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
			zxfer_throw_error "Failed to diff source and destination snapshots for recursive delta planning." "$l_status"
		}
	fi

	if [ -s "$l_missing_snapshots_tmp_file" ]; then
		zxfer_capture_recursive_dataset_list_from_snapshot_file "$l_missing_snapshots_tmp_file" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
			zxfer_throw_error "Failed to derive recursive source dataset transfer list." "$l_status"
		}
		g_recursive_source_list=$g_zxfer_recursive_dataset_list_result
	else
		g_recursive_source_list=""
	fi
	if [ -s "$l_destination_extra_snapshots_tmp_file" ]; then
		zxfer_capture_recursive_dataset_list_from_snapshot_file "$l_destination_extra_snapshots_tmp_file" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
			zxfer_throw_error "Failed to derive recursive destination dataset delete list." "$l_status"
		}
		g_recursive_destination_extra_dataset_list=$g_zxfer_recursive_dataset_list_result
	else
		g_recursive_destination_extra_dataset_list=""
	fi
	if zxfer_snapshot_discovery_needs_source_dataset_inventory; then
		zxfer_capture_recursive_dataset_list_from_snapshot_file "$l_source_snaps_diff_input_file" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
			zxfer_throw_error "Failed to derive recursive source dataset inventory." "$l_status"
		}
		g_recursive_source_dataset_list=$g_zxfer_recursive_dataset_list_result
	else
		g_recursive_source_dataset_list=""
	fi

	# if excluding datasets, remove them from the list
	if [ "$g_option_x_exclude_datasets" != "" ]; then
		zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_source_list" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
			zxfer_throw_error "Failed to filter recursive source dataset transfer list against exclude patterns." "$l_status"
		}
		g_recursive_source_list=$g_zxfer_recursive_dataset_list_result
		zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_destination_extra_dataset_list" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
			zxfer_throw_error "Failed to filter recursive destination dataset delete list against exclude patterns." "$l_status"
		}
		g_recursive_destination_extra_dataset_list=$g_zxfer_recursive_dataset_list_result
		if zxfer_snapshot_discovery_needs_source_dataset_inventory; then
			zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_source_dataset_list" || {
				l_status=$?
				zxfer_cleanup_runtime_artifact_path_list "$l_delta_stage_files"
				zxfer_throw_error "Failed to filter recursive source dataset inventory against exclude patterns." "$l_status"
			}
			g_recursive_source_dataset_list=$g_zxfer_recursive_dataset_list_result
		fi
	fi

	# debugging
	if [ "$g_option_V_very_verbose" -eq 1 ]; then
		echo "====================================================================="
		echo "====== Snapshots present in source but missing in destination ======"
		if [ -s "$l_missing_snapshots_tmp_file" ]; then
			cat "$l_missing_snapshots_tmp_file"
		fi
		echo "====== Source datasets that differ from destination ======"
		echo "g_recursive_source_list:"
		echo "$g_recursive_source_list"
		echo "Source dataset count: $(echo "$g_recursive_source_list" | grep -cve '^\s*$')"
		echo "====================================================================="
		echo "====== Extra Destination snapshots not in source ======"
		if [ -s "$l_destination_extra_snapshots_tmp_file" ]; then
			cat "$l_destination_extra_snapshots_tmp_file"
		fi
		echo "====== Destination datasets with extra snapshots not in source ======"
		if [ "$g_recursive_destination_extra_dataset_list" != "" ]; then
			printf '%s\n' "$g_recursive_destination_extra_dataset_list"
		fi
		echo "====================================================================="
	fi

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

# Purpose: Decide whether local recursive destination dataset inventory should
# be collected after snapshot diffing.
# Usage: Called after recursive snapshot deltas are known so no-op runs avoid
# building a destination existence cache that no later stage can consume.
zxfer_snapshot_discovery_needs_destination_dataset_inventory() {
	if [ -n "${g_option_T_target_host:-}" ]; then
		return 1
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

# Purpose: Try to prove a clean recursive no-op (local or remote-origin source)
# with identity-aware discovery before the full creation-order source listing.
# Usage: Called by zxfer_get_zfs_list; returns 0 when no-op was proven and the
# caller can return, returns 1 when the normal discovery path should continue.
zxfer_try_fast_recursive_noop_discovery() {
	zxfer_fast_recursive_noop_discovery_is_eligible || return 1

	g_source_snapshot_fast_noop_attempted=1
	l_source_fifo=""
	l_destination_fifo=""
	l_source_err_file=""
	l_source_cmd_tmp_file=""
	l_source_count_file=""
	l_dest_snapshot_err_file=""
	l_dest_snapshot_status_file=""
	l_dest_normalize_status_file=""
	l_dest_stream_status_file=""
	l_cmp_diff_file=""
	l_source_name_list_uses_parallel=0
	l_fast_stage_files=""

	zxfer_create_temp_file_group 9 >/dev/null || return "$?"
	l_fast_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_source_fifo
		IFS= read -r l_destination_fifo
		IFS= read -r l_source_err_file
		IFS= read -r l_source_cmd_tmp_file
		IFS= read -r l_source_count_file
		IFS= read -r l_dest_snapshot_err_file
		IFS= read -r l_dest_snapshot_status_file
		IFS= read -r l_dest_normalize_status_file
		IFS= read -r l_dest_stream_status_file
	} <<-EOF
		$l_fast_stage_files
	EOF

	l_source_snapshot_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		l_source_snapshot_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi
	zxfer_build_source_snapshot_name_list_cmd >"$l_source_cmd_tmp_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
		return "$l_status"
	}
	zxfer_read_source_snapshot_discovery_command_file "$l_source_cmd_tmp_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
		return "$l_status"
	}
	l_cmd=$g_zxfer_snapshot_discovery_file_read_result
	if [ -z "$l_cmd" ]; then
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
		zxfer_throw_error "Staged source snapshot no-op proof command was empty."
	fi
	l_cmp_diff_file=$l_source_cmd_tmp_file
	if [ "${g_source_snapshot_list_uses_parallel:-0}" -eq 1 ]; then
		l_source_name_list_uses_parallel=1
	fi
	g_source_snapshot_list_cmd=$l_cmd
	zxfer_profile_increment_counter g_zxfer_profile_source_snapshot_list_commands
	if [ "$l_source_name_list_uses_parallel" -eq 1 ]; then
		zxfer_profile_increment_counter g_zxfer_profile_source_snapshot_list_parallel_commands
	fi
	if [ "$g_option_O_origin_host" != "" ]; then
		zxfer_profile_record_ssh_invocation "$g_option_O_origin_host" source
	fi

	# Stage into regular temp files. FIFO comparisons can strand a producer
	# when the compare command exits or cannot open both streams.
	zxfer_execute_source_snapshot_name_list_background_sort_cmd \
		"$l_cmd" "$l_source_fifo" "$l_source_err_file" "$l_source_count_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
		return "$l_status"
	}
	l_source_snapshot_pid=$g_last_background_pid

	l_destination_snapshot_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		l_destination_snapshot_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi
	zxfer_start_destination_snapshot_name_sorted_fifo_producer \
		"$l_destination_fifo" \
		"$l_dest_snapshot_err_file" \
		"$l_dest_snapshot_status_file" \
		"$l_dest_normalize_status_file" \
		"$l_dest_stream_status_file" || {
		l_status=$?
		zxfer_abort_fast_noop_background_pid "$l_source_snapshot_pid" "background source snapshot no-op proof helper"
		wait "$l_source_snapshot_pid" 2>/dev/null || :
		zxfer_unregister_cleanup_pid "$l_source_snapshot_pid"
		g_last_background_pid=""
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
		return "$l_status"
	}
	l_destination_snapshot_pid=$g_last_background_pid

	l_source_snapshot_wait_status=0
	wait "$l_source_snapshot_pid" || l_source_snapshot_wait_status=$?
	zxfer_unregister_cleanup_pid "$l_source_snapshot_pid"
	zxfer_profile_add_elapsed_ms g_zxfer_profile_source_snapshot_listing_ms "$l_source_snapshot_stage_start_ms"
	l_destination_snapshot_wait_status=0
	wait "$l_destination_snapshot_pid" || l_destination_snapshot_wait_status=$?
	zxfer_unregister_cleanup_pid "$l_destination_snapshot_pid"
	g_last_background_pid=""
	zxfer_profile_add_elapsed_ms g_zxfer_profile_destination_snapshot_listing_ms "$l_destination_snapshot_stage_start_ms"

	l_snapshot_diff_sort_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		l_snapshot_diff_sort_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi
	l_cmp_status=0
	if LC_ALL=C comm -3 "$l_source_fifo" "$l_destination_fifo" >"$l_cmp_diff_file"; then
		if [ -s "$l_cmp_diff_file" ]; then
			l_cmp_status=1
		fi
	else
		l_cmp_status=$?
	fi
	zxfer_profile_add_elapsed_ms g_zxfer_profile_snapshot_diff_sort_ms "$l_snapshot_diff_sort_stage_start_ms"

	if [ "$l_cmp_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
		if [ "$l_cmp_status" -eq 1 ]; then
			zxfer_reset_destination_existence_cache
			return 1
		fi
		zxfer_throw_error "Failed to compare source and destination snapshots for recursive no-op proof." "$l_cmp_status"
	fi

	l_destination_status_read_status=0
	zxfer_read_snapshot_discovery_status_file "$l_dest_snapshot_status_file" 1 ||
		l_destination_status_read_status=$?
	l_list_status=$g_zxfer_snapshot_discovery_status_file_result
	zxfer_read_snapshot_discovery_status_file "$l_dest_normalize_status_file" 1 ||
		l_destination_status_read_status=$?
	l_normalize_status=$g_zxfer_snapshot_discovery_status_file_result
	zxfer_read_snapshot_discovery_status_file "$l_dest_stream_status_file" 1 ||
		l_destination_status_read_status=$?
	l_dest_stream_status=$g_zxfer_snapshot_discovery_status_file_result
	if [ "$l_destination_status_read_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
		zxfer_throw_error "Failed to validate destination snapshot status for recursive no-op proof." "$l_destination_status_read_status"
	fi
	l_missing_destination=0
	if [ "$l_list_status" -ne 0 ]; then
		zxfer_read_snapshot_discovery_capture_file "$l_dest_snapshot_err_file" || {
			l_read_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
			zxfer_throw_error "Failed to read staged destination snapshot stderr." "$l_read_status"
		}
		l_dest_snapshot_err=$g_zxfer_snapshot_discovery_file_read_result
		if zxfer_destination_probe_reports_missing "$l_dest_snapshot_err"; then
			l_missing_destination=1
		else
			if [ -n "$l_dest_snapshot_err" ]; then
				printf '%s\n' "$l_dest_snapshot_err" >&2
			fi
			zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
			zxfer_throw_error "Failed to retrieve snapshot list from the destination." "$l_list_status"
		fi
	fi
	for l_status in "$l_normalize_status" "$l_dest_stream_status" "$l_destination_snapshot_wait_status"; do
		[ "$l_status" -eq 0 ] && continue
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
		return "$l_status"
	done

	if [ "$l_source_snapshot_wait_status" -ne 0 ]; then
		if [ -n "${g_source_snapshot_list_cmd:-}" ]; then
			zxfer_record_last_command_string "$g_source_snapshot_list_cmd"
		fi
		zxfer_read_snapshot_discovery_capture_file "$l_source_err_file" || {
			l_source_stderr_read_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
			zxfer_throw_error "Failed to read staged source snapshot stderr." "$l_source_stderr_read_status"
		}
		l_source_snapshot_err=$g_zxfer_snapshot_discovery_file_read_result
		l_source_snapshot_err=$(zxfer_limit_snapshot_discovery_capture_lines \
			"$l_source_snapshot_err" 10)
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
		if [ "$l_source_snapshot_err" != "" ]; then
			zxfer_throw_error "Failed to retrieve snapshots from the source: $l_source_snapshot_err" "$l_source_snapshot_wait_status"
		fi
		zxfer_throw_error "Failed to retrieve snapshots from the source" "$l_source_snapshot_wait_status"
	fi

	l_source_count_status=0
	zxfer_read_snapshot_discovery_status_file "$l_source_count_file" 1 ||
		l_source_count_status=$?
	l_source_snapshot_count=$g_zxfer_snapshot_discovery_status_file_result
	if [ "$l_source_count_status" -ne 0 ] || [ "$l_source_snapshot_count" -ne 1 ]; then
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
		if [ -n "${g_option_x_exclude_datasets:-}" ]; then
			zxfer_reset_destination_existence_cache
			return 1
		fi
		zxfer_throw_error "Failed to retrieve snapshots from the source"
	fi

	if [ "$l_missing_destination" -eq 1 ]; then
		zxfer_echoV "Destination dataset does not exist: $(zxfer_get_destination_snapshot_root_dataset)"
		zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
		zxfer_reset_destination_existence_cache
		return 1
	fi

	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_recursive_destination_extra_dataset_list=""
	g_recursive_dest_list=""
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	zxfer_cleanup_runtime_artifact_path_list "$l_fast_stage_files"
	zxfer_echov "No new snapshots to transfer."
	return 0
}

# Purpose: Build the source and destination snapshot inventories that the rest
# of replication planning depends on.
# Usage: Called during source and destination snapshot discovery near the start
# of each live pass so later delete, seed, and send decisions work from one
# shared discovery result.
#
# Build the source and destination snapshot caches used by replication.
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
	zxfer_reset_snapshot_record_indexes

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		zxfer_echoV "Dry run: skipping live snapshot discovery for $g_initial_source -> $g_destination."
		zxfer_echoV "End zxfer_get_zfs_list()"
		return
	fi

	l_fast_noop_status=0
	zxfer_try_fast_recursive_noop_discovery || l_fast_noop_status=$?
	if [ "$l_fast_noop_status" -eq 0 ]; then
		zxfer_echoV "End zxfer_get_zfs_list()"
		return
	fi
	if [ "$l_fast_noop_status" -ne 1 ]; then
		return "$l_fast_noop_status"
	fi

	# create temporary files used by the background processes
	zxfer_create_temp_file_group 2 >/dev/null || return "$?"
	l_source_snapshot_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_lzfs_list_hr_s_snap_tmp_file
		IFS= read -r l_lzfs_list_hr_s_snap_err_tmp_file
	} <<-EOF
		$l_source_snapshot_stage_files
	EOF

	#
	# BEGIN background process
	#
	g_source_snapshot_list_pid=""
	g_source_snapshot_list_job_id=""
	l_lzfs_list_hr_s_snap_sorted_tmp_file=""
	l_source_snapshot_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		l_source_snapshot_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi
	l_status=0
	g_source_snapshot_list_background_sort_requested=1
	zxfer_write_source_snapshot_list_to_file "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" ||
		l_status=$?
	g_source_snapshot_list_background_sort_requested=0
	l_lzfs_list_hr_s_snap_sorted_tmp_file=${g_source_snapshot_list_sorted_file:-}
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_lzfs_list_hr_s_snap_sorted_tmp_file"
		g_source_snapshot_list_sorted_file=""
		zxfer_cleanup_runtime_artifact_path_list_and_return "$l_status" "$l_source_snapshot_stage_files"
		return "$?"
	fi

	#
	# Run as many commands prior to the wait command as possible.
	#

	l_destination_snapshot_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		l_destination_snapshot_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi
	l_destination_inventory_attempted=0
	l_destination_dataset=$(zxfer_get_destination_snapshot_root_dataset)
	zxfer_get_temp_file >/dev/null || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file"
		zxfer_cleanup_snapshot_record_cache_files
		return "$l_status"
	}
	l_rzfs_list_hr_snap_tmp_file=$g_zxfer_temp_file_result
	zxfer_get_temp_file >/dev/null || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
			"$l_rzfs_list_hr_snap_tmp_file"
		zxfer_cleanup_snapshot_record_cache_files
		return "$l_status"
	}
	l_dest_snaps_stripped_sorted_tmp_file=$g_zxfer_temp_file_result

	if [ -n "${g_option_T_target_host:-}" ]; then
		zxfer_create_temp_file_group 2 >/dev/null || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
				"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			return "$l_status"
		}
		l_destination_inventory_stage_files=$g_zxfer_temp_file_group_result
		{
			IFS= read -r l_dest_list_tmp_file
			IFS= read -r l_dest_list_err_file
		} <<-EOF
			$l_destination_inventory_stage_files
		EOF
		if zxfer_command_display_render_enabled; then
			l_cmd=$(zxfer_render_destination_zfs_command list -t filesystem,volume -Hr -o name "$g_destination")
			zxfer_echoV "Running command: $l_cmd"
			zxfer_record_last_command_string "$l_cmd"
		else
			zxfer_record_last_command_opaque
		fi
		l_rzfs_list_hr_snap_err_tmp_file=""
		zxfer_get_temp_file >/dev/null || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_path_list "$l_destination_inventory_stage_files"
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
				"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			return "$l_status"
		}
		l_rzfs_list_hr_snap_err_tmp_file=$g_zxfer_temp_file_result

		l_dest_inventory_status=0
		zxfer_run_remote_destination_discovery_batch_to_files \
			"$l_destination_dataset" \
			"$l_dest_list_tmp_file" \
			"$l_dest_list_err_file" \
			"$l_rzfs_list_hr_snap_tmp_file" \
			"$l_rzfs_list_hr_snap_err_tmp_file" ||
			l_dest_inventory_status=$?
		if [ "$l_dest_inventory_status" -eq 0 ]; then
			l_dest_inventory_status=$g_zxfer_destination_discovery_batch_inventory_status
		fi
		zxfer_publish_destination_dataset_inventory_from_stage \
			"$l_dest_list_tmp_file" \
			"$l_dest_list_err_file" \
			"$l_dest_inventory_status" \
			"${g_zxfer_destination_discovery_batch_pool_status:-}"
		l_destination_inventory_attempted=1
		if [ "${g_zxfer_destination_discovery_batch_snapshot_status:-0}" -ne 0 ]; then
			l_snapshot_stderr_read_status=0
			zxfer_read_snapshot_discovery_capture_file "$l_rzfs_list_hr_snap_err_tmp_file" ||
				l_snapshot_stderr_read_status=$?
			l_snapshot_stderr=$g_zxfer_snapshot_discovery_file_read_result
			zxfer_cleanup_runtime_artifact_path_list "$l_destination_inventory_stage_files"
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
				"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" "$l_rzfs_list_hr_snap_err_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			if [ "$l_snapshot_stderr_read_status" -ne 0 ]; then
				zxfer_throw_error "Failed to read staged destination snapshot stderr." "$l_snapshot_stderr_read_status"
			fi
			if [ "$l_snapshot_stderr" != "" ]; then
				zxfer_warn_stderr "$l_snapshot_stderr"
			fi
			zxfer_throw_error "Failed to retrieve snapshot list from the destination." "$g_zxfer_destination_discovery_batch_snapshot_status"
		fi
		zxfer_cleanup_runtime_artifact_path_list "$l_destination_inventory_stage_files"
		zxfer_normalize_destination_snapshot_list "$l_destination_dataset" "$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
				"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" "$l_rzfs_list_hr_snap_err_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			return "$l_status"
		}
		zxfer_cleanup_runtime_artifact_path "$l_rzfs_list_hr_snap_err_tmp_file"
	else
		# this function writes to both files passed as parameters
		zxfer_write_destination_snapshot_list_to_files "$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
				"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			return "$l_status"
		}
	fi
	zxfer_profile_add_elapsed_ms g_zxfer_profile_destination_snapshot_listing_ms "$l_destination_snapshot_stage_start_ms"

	zxfer_echoV "Waiting for background processes to finish."
	l_source_snapshot_wait_status=0
	l_source_snapshot_wait_report_failure=""
	if [ -n "${g_source_snapshot_list_job_id:-}" ]; then
		zxfer_wait_for_background_job "$g_source_snapshot_list_job_id" || {
			l_wait_helper_status=$?
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
				"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			zxfer_throw_error "Failed to read source snapshot discovery completion metadata." "$l_wait_helper_status"
		}
		l_source_snapshot_wait_status=$g_zxfer_background_job_wait_exit_status
		l_source_snapshot_wait_report_failure=${g_zxfer_background_job_wait_report_failure:-}
		g_source_snapshot_list_pid=""
		g_source_snapshot_list_job_id=""
	elif [ -n "${g_source_snapshot_list_pid:-}" ]; then
		wait "$g_source_snapshot_list_pid" || l_source_snapshot_wait_status=$?
		zxfer_unregister_cleanup_pid "$g_source_snapshot_list_pid"
		g_source_snapshot_list_pid=""
	fi
	zxfer_profile_add_elapsed_ms g_zxfer_profile_source_snapshot_listing_ms "$l_source_snapshot_stage_start_ms"

	case $l_source_snapshot_wait_report_failure in
	queue_write)
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
			"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
		zxfer_cleanup_snapshot_record_cache_files
		zxfer_throw_error "Failed to publish source snapshot discovery completion."
		;;
	completion_write)
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
			"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
		zxfer_cleanup_snapshot_record_cache_files
		zxfer_throw_error "Failed to report source snapshot discovery completion."
		;;
	esac

	if [ "$l_source_snapshot_wait_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
			"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
		zxfer_cleanup_snapshot_record_cache_files
		if [ -n "${g_source_snapshot_list_cmd:-}" ]; then
			zxfer_record_last_command_string "$g_source_snapshot_list_cmd"
		fi
		zxfer_read_snapshot_discovery_capture_file "$l_lzfs_list_hr_s_snap_err_tmp_file" || {
			l_source_stderr_read_status=$?
			zxfer_cleanup_runtime_artifact_path "$l_lzfs_list_hr_s_snap_err_tmp_file"
			zxfer_throw_error "Failed to read staged source snapshot stderr." "$l_source_stderr_read_status"
		}
		l_source_snapshot_err=$g_zxfer_snapshot_discovery_file_read_result
		l_source_snapshot_err=$(zxfer_limit_snapshot_discovery_capture_lines \
			"$l_source_snapshot_err" 10)
		zxfer_cleanup_runtime_artifact_path "$l_lzfs_list_hr_s_snap_err_tmp_file"
		if [ "$l_source_snapshot_err" != "" ]; then
			zxfer_throw_error "Failed to retrieve snapshots from the source: $l_source_snapshot_err" "$l_source_snapshot_wait_status"
		fi
		zxfer_throw_error "Failed to retrieve snapshots from the source" "$l_source_snapshot_wait_status"
	fi
	zxfer_echoV "Background processes finished."

	if [ ! -s "$l_lzfs_list_hr_s_snap_tmp_file" ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
			"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
		zxfer_cleanup_snapshot_record_cache_files
		zxfer_throw_error "Failed to retrieve snapshots from the source"
	fi

	#
	# END background process
	#
	l_snapshot_diff_sort_stage_start_ms=""
	if zxfer_profile_metrics_enabled; then
		l_snapshot_diff_sort_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	fi
	l_status=0
	zxfer_set_g_recursive_source_list "$l_lzfs_list_hr_s_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" "$l_lzfs_list_hr_s_snap_sorted_tmp_file" ||
		l_status=$?
	zxfer_profile_add_elapsed_ms g_zxfer_profile_snapshot_diff_sort_ms "$l_snapshot_diff_sort_stage_start_ms"
	zxfer_cleanup_runtime_artifact_paths "$l_dest_snaps_stripped_sorted_tmp_file"
	zxfer_cleanup_runtime_artifact_path "$l_lzfs_list_hr_s_snap_sorted_tmp_file"
	g_source_snapshot_list_sorted_file=""
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
			"$l_rzfs_list_hr_snap_tmp_file"
		zxfer_cleanup_snapshot_record_cache_files
		return "$l_status"
	fi

	if zxfer_snapshot_discovery_needs_destination_dataset_inventory; then
		zxfer_collect_local_destination_dataset_inventory || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
				"$l_rzfs_list_hr_snap_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			return "$l_status"
		}
		l_destination_inventory_attempted=1
	fi

	if zxfer_snapshot_discovery_needs_record_caches; then
		g_zxfer_destination_snapshot_record_cache_file=$l_rzfs_list_hr_snap_tmp_file
		zxfer_read_snapshot_discovery_capture_file "$l_rzfs_list_hr_snap_tmp_file" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
				"$l_lzfs_list_hr_s_snap_err_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			zxfer_throw_error "Failed to read staged destination snapshot list." "$l_status"
		}
		g_rzfs_list_hr_snap=$g_zxfer_snapshot_discovery_file_read_result

		zxfer_read_snapshot_discovery_capture_file "$l_lzfs_list_hr_s_snap_tmp_file" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
				"$l_lzfs_list_hr_s_snap_err_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			zxfer_throw_error "Failed to read staged source snapshot list." "$l_status"
		}
		g_lzfs_list_hr_snap=$g_zxfer_snapshot_discovery_file_read_result
		zxfer_get_temp_file >/dev/null || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
				"$l_lzfs_list_hr_s_snap_err_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			return "$l_status"
		}
		l_source_snapshot_record_cache_file=$g_zxfer_temp_file_result
		if zxfer_command_display_render_enabled; then
			l_cmd="$(zxfer_render_command_for_report "" zxfer_reverse_file_lines "$l_lzfs_list_hr_s_snap_tmp_file") > $(zxfer_quote_token_for_report "$l_source_snapshot_record_cache_file")"
			zxfer_echoV "Running command: $l_cmd"
			zxfer_record_last_command_string "$l_cmd"
		else
			zxfer_record_last_command_opaque
		fi
		zxfer_reverse_file_lines "$l_lzfs_list_hr_s_snap_tmp_file" >"$l_source_snapshot_record_cache_file" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
				"$l_source_snapshot_record_cache_file"
			zxfer_cleanup_snapshot_record_cache_files
			zxfer_throw_error "Failed to stage source snapshot record cache." "$l_status"
		}
		g_zxfer_source_snapshot_record_cache_file=$l_source_snapshot_record_cache_file
		zxfer_cleanup_runtime_artifact_path "$l_lzfs_list_hr_s_snap_tmp_file"
	else
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
			"$l_rzfs_list_hr_snap_tmp_file"
	fi

	zxfer_cleanup_runtime_artifact_path "$l_lzfs_list_hr_s_snap_err_tmp_file"

	#
	# Errors
	#

	if [ "$l_destination_inventory_attempted" -eq 1 ] && [ "$g_recursive_dest_list" = "" ]; then
		zxfer_echoV "Destination dataset list is empty; assuming no existing datasets under \"$g_destination\""
	fi

	zxfer_echoV "End zxfer_get_zfs_list()"
}
