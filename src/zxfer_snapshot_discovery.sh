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
# owns globals: recursive snapshot-discovery state, current-shell staged-file and recursive dataset-list scratch, and metadata-compression state.
# reads globals: g_option_j_jobs, g_option_O_origin_host, g_option_z_compress, g_LZFS/g_RZFS, and remote helper paths.
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

	g_zxfer_source_snapshot_record_cache_file=""
	g_zxfer_destination_snapshot_record_cache_file=""
}

# Purpose: Reset the snapshot discovery state so the next snapshot-discovery
# pass starts from a clean state.
# Usage: Called during source and destination snapshot discovery before this
# module reuses mutable scratch globals or cached decisions.
zxfer_reset_snapshot_discovery_state() {
	zxfer_cleanup_snapshot_record_cache_files
	g_source_snapshot_list_cmd=""
	g_source_snapshot_list_pid=""
	g_source_snapshot_list_uses_parallel=0
	g_source_snapshot_list_uses_metadata_compression=0
	g_origin_parallel_cmd=""
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_recursive_dest_list=""
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_snapshot_discovery_file_read_result=""
	g_zxfer_parallel_source_job_check_result=""
	g_zxfer_parallel_source_job_check_kind=""
	g_zxfer_recursive_dataset_list_result=""
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
	if zxfer_read_runtime_artifact_file "$l_capture_path" >/dev/null; then
		g_zxfer_snapshot_discovery_file_read_result=$g_zxfer_runtime_artifact_read_result
	else
		l_read_status=$?
		return "$l_read_status"
	fi

	return 0
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

	if zxfer_ensure_parallel_available_for_source_jobs >/dev/null 2>&1; then
		l_status=0
	else
		l_status=$?
	fi

	return "$l_status"
}

# Purpose: Check whether the version output reports gnu parallel.
# Usage: Called during source and destination snapshot discovery when later
# helpers need a boolean answer about the version output.
#
# Accept GNU parallel signatures anywhere in the version output, because some
# installs emit citation or warning text before the canonical banner.
zxfer_version_output_reports_gnu_parallel() {
	l_output=$1
	l_normalized_output=$(printf '%s' "$l_output" |
		tr '[:upper:]' '[:lower:]' |
		tr '\r\n\t' '   ')

	case "$l_normalized_output" in
	*gnu\ parallel*)
		return 0
		;;
	esac

	return 1
}

# Purpose: Check whether the local parallel functional probe reports gnu.
# Usage: Called during source and destination snapshot discovery when later
# helpers need a boolean answer about the local parallel functional probe.
#
# Some real GNU parallel builds are wrapped or repackaged in ways that alter the
# version banner. When the banner check is inconclusive, run a tiny GNU-specific
# placeholder command before rejecting the helper.
zxfer_local_parallel_functional_probe_reports_gnu() {
	l_parallel_path=$1
	l_probe_sentinel="zxfer-gnu-parallel-probe"

	[ -n "$l_parallel_path" ] || return 1

	l_probe_output=$(
		printf '%s\n' "$l_probe_sentinel" |
			"$l_parallel_path" --will-cite -j 1 --line-buffer -- "printf '%s\n' {}" 2>/dev/null
	) || return 1

	[ "$l_probe_output" = "$l_probe_sentinel" ]
}

# Purpose: Return the local parallel version output in the form expected by
# later helpers.
# Usage: Called during source and destination snapshot discovery when sibling
# helpers need the same lookup without duplicating module logic.
zxfer_get_local_parallel_version_output() {
	l_parallel_path=$1

	[ -n "$l_parallel_path" ] || return 1

	l_version_output=$("$l_parallel_path" --will-cite --version 2>&1)
	l_version_status=$?
	if [ "$l_version_status" -ne 0 ] ||
		[ "$l_version_output" = "" ] ||
		! zxfer_version_output_reports_gnu_parallel "$l_version_output"; then
		l_version_output=$("$l_parallel_path" --version 2>&1)
	fi

	printf '%s\n' "$l_version_output"
}

# Purpose: Ensure the parallel available for source jobs exists and is ready
# before the flow continues.
# Usage: Called during source and destination snapshot discovery before later
# helpers assume the resource or cache is available.
#
# Ensure GNU parallel exists locally (for piping) and resolve the remote path
# when -j is used with -O. Missing helpers still fall back to serial discovery,
# while resolved remote helpers are trusted and fail closed later if execution
# proves they are incompatible.
zxfer_ensure_parallel_available_for_source_jobs() {
	g_zxfer_parallel_source_job_check_result=""
	g_zxfer_parallel_source_job_check_kind=""

	if [ "$g_option_j_jobs" -le 1 ]; then
		return 0
	fi

	if [ "$g_option_O_origin_host" = "" ]; then
		if [ "$g_cmd_parallel" = "" ]; then
			g_zxfer_parallel_source_job_check_kind="local_missing"
			g_zxfer_parallel_source_job_check_result="The -j option requires GNU parallel but it was not found in PATH on the local host."
			printf '%s\n' "$g_zxfer_parallel_source_job_check_result"
			return 1
		fi

		# Ensure we are using GNU parallel and not a differing implementation.
		l_parallel_version=$(zxfer_get_local_parallel_version_output "$g_cmd_parallel")
		if ! zxfer_version_output_reports_gnu_parallel "$l_parallel_version" &&
			! zxfer_local_parallel_functional_probe_reports_gnu "$g_cmd_parallel"; then
			g_zxfer_parallel_source_job_check_kind="local_non_gnu"
			g_zxfer_parallel_source_job_check_result="The -j option requires GNU parallel, but \"$g_cmd_parallel\" is not GNU parallel."
			printf '%s\n' "$g_zxfer_parallel_source_job_check_result"
			return 1
		fi

		return 0
	fi

	if ! l_remote_parallel=$(zxfer_resolve_remote_required_tool "$g_option_O_origin_host" parallel "GNU parallel" source); then
		case "$l_remote_parallel" in
		"Required dependency \"GNU parallel\" not found on host "*)
			g_zxfer_parallel_source_job_check_kind="origin_missing"
			g_zxfer_parallel_source_job_check_result="GNU parallel not found on origin host $g_option_O_origin_host but -j $g_option_j_jobs was requested. Install GNU parallel remotely or rerun without -j."
			;;
		*)
			g_zxfer_parallel_source_job_check_kind="origin_probe_failed"
			g_zxfer_parallel_source_job_check_result=$l_remote_parallel
			;;
		esac
		printf '%s\n' "$g_zxfer_parallel_source_job_check_result"
		return 1
	fi

	if [ "${g_origin_parallel_cmd:-}" != "$l_remote_parallel" ]; then
		g_origin_parallel_cmd=$l_remote_parallel
	fi

	return 0
}

# Purpose: Return the source snapshot parallel dataset threshold in the form
# expected by later helpers.
# Usage: Called during source and destination snapshot discovery when sibling
# helpers need the same lookup without duplicating module logic.
#
# Choose how many datasets must exist before the more expensive per-dataset GNU
# parallel discovery path becomes worthwhile. Remote startup stays biased
# toward the single recursive list until the tree is large enough to repay the
# extra process fan-out.
zxfer_get_source_snapshot_parallel_dataset_threshold() {
	l_jobs=${g_option_j_jobs:-1}

	case "$l_jobs" in
	'' | *[!0-9]*)
		l_jobs=1
		;;
	esac

	if [ "$g_option_O_origin_host" = "" ]; then
		l_threshold=$((l_jobs * 2))
		[ "$l_threshold" -lt 8 ] && l_threshold=8
		printf '%s\n' "$l_threshold"
		return 0
	fi

	case "${g_origin_remote_capabilities_bootstrap_source:-}" in
	cache | memory)
		l_threshold=$((l_jobs * 2))
		[ "$l_threshold" -lt 6 ] && l_threshold=6
		;;
	*)
		l_threshold=$((l_jobs * 3))
		[ "$l_threshold" -lt 12 ] && l_threshold=12
		;;
	esac

	if [ "${g_ssh_supports_control_sockets:-0}" -ne 1 ] ||
		[ -z "${g_ssh_origin_control_socket:-}" ]; then
		l_threshold=$((l_threshold + l_jobs))
	fi

	printf '%s\n' "$l_threshold"
}

# Purpose: Return the source snapshot discovery dataset list in the form
# expected by later helpers.
# Usage: Called during source and destination snapshot discovery when sibling
# helpers need the same lookup without duplicating module logic.
zxfer_get_source_snapshot_discovery_dataset_list() {
	zxfer_run_source_zfs_cmd list -Hr -t filesystem,volume -o name "$g_initial_source"
}

# Purpose: Count the source snapshot discovery datasets for the surrounding
# snapshot-discovery flow.
# Usage: Called during source and destination snapshot discovery when later
# helpers need a threshold or size decision.
zxfer_count_source_snapshot_discovery_datasets() {
	l_dataset_list=$1
	if [ "$l_dataset_list" = "" ]; then
		printf '%s\n' "0"
		return 0
	fi
	l_dataset_count=$(printf '%s\n' "$l_dataset_list" | "$g_cmd_awk" 'END { print NR + 0 }')

	case "$l_dataset_count" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	printf '%s\n' "$l_dataset_count"
}

# Purpose: Check whether zxfer should inline source snapshot dataset list.
# Usage: Called during source and destination snapshot discovery when later
# helpers need a boolean branch decision about the current configuration or
# live state.
zxfer_should_inline_source_snapshot_dataset_list() {
	l_dataset_list=$1
	l_dataset_count=$2

	case "$l_dataset_count" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	[ "$l_dataset_count" -le 64 ] || return 1

	l_dataset_list_bytes=$(printf '%s' "$l_dataset_list" | wc -c | tr -d '[:space:]')
	case "$l_dataset_list_bytes" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	[ "$l_dataset_list_bytes" -le 8192 ]
}

# Purpose: Build the source snapshot dataset list printf command for the next
# execution or comparison step.
# Usage: Called during source and destination snapshot discovery before other
# helpers consume the assembled value.
zxfer_build_source_snapshot_dataset_list_printf_cmd() {
	l_dataset_list=$1
	l_tokens=$(printf '%s\n%s' "printf" "%s\n")

	while IFS= read -r l_dataset || [ -n "$l_dataset" ]; do
		[ "$l_dataset" = "" ] && continue
		l_tokens=$(printf '%s\n%s' "$l_tokens" "$l_dataset")
	done <<EOF
$l_dataset_list
EOF

	zxfer_quote_token_stream "$l_tokens"
}

# Purpose: Record the parallel source discovery fallback for later diagnostics
# or control decisions.
# Usage: Called during source and destination snapshot discovery when zxfer
# needs the state preserved for follow-on helpers or reporting.
zxfer_note_parallel_source_discovery_fallback() {
	l_reason=$1
	l_scope=${2:-local}

	if [ "${g_option_v_verbose:-0}" -ne 1 ] &&
		[ "${g_option_V_very_verbose:-0}" -ne 1 ]; then
		return 0
	fi

	case "$l_scope" in
	origin)
		printf '%s\n' "Origin-host GNU parallel unavailable for adaptive source discovery. Falling back to serial source snapshot listing." >&2
		;;
	*)
		printf '%s\n' "GNU parallel unavailable for adaptive source discovery. Falling back to serial source snapshot listing." >&2
		;;
	esac

	[ -z "$l_reason" ] || printf '%s\n' "$l_reason" >&2
}

# Purpose: Check whether the parallel source discovery fallback is allowed.
# Usage: Called during source and destination snapshot discovery when later
# helpers need a boolean answer about the parallel source discovery fallback.
#
# Local adaptive discovery can always degrade to serial when the helper check
# fails, but remote adaptive discovery should only do so for the explicit
# missing-helper case. Transport, bootstrap, or other probe failures must stop
# the run instead of silently masking the origin-host problem.
zxfer_parallel_source_discovery_fallback_is_allowed() {
	l_kind=$1
	l_scope=${2:-local}

	case "$l_scope" in
	local)
		return 0
		;;
	origin)
		case "$l_kind" in
		origin_missing)
			return 0
			;;
		esac
		return 1
		;;
	esac

	return 1
}

# Purpose: Render the remote source snapshot serial list command as a stable
# shell-safe or operator-facing string.
# Usage: Called during source and destination snapshot discovery when zxfer
# needs to display or transport the value without reparsing it.
zxfer_render_remote_source_snapshot_serial_list_cmd() {
	l_remote_zfs_cmd=${g_origin_cmd_zfs:-$g_cmd_zfs}
	l_remote_serial_cmd=$(zxfer_build_shell_command_from_argv \
		"$l_remote_zfs_cmd" list -Hr -o name -s creation -t snapshot "$g_initial_source")
	if [ "$g_option_z_compress" -eq 1 ]; then
		g_source_snapshot_list_uses_metadata_compression=1
		l_remote_compress_safe=${g_origin_cmd_compress_safe:-$g_cmd_compress_safe}
		l_remote_serial_cmd="$l_remote_serial_cmd | $l_remote_compress_safe"
	fi
	l_remote_shell_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_serial_cmd")
	l_cmd=$(zxfer_build_ssh_shell_command_for_host "$g_option_O_origin_host" "$l_remote_shell_cmd") || return 1
	if [ "${g_source_snapshot_list_uses_metadata_compression:-0}" -eq 1 ]; then
		l_cmd="$l_cmd | $g_cmd_decompress_safe"
	fi
	printf '%s\n' "$l_cmd"
}

# Purpose: Build the source snapshot list command for the next execution or
# comparison step.
# Usage: Called during source and destination snapshot discovery before other
# helpers consume the assembled value.
#
# Build the ZFS list command used to enumerate source snapshots based on the
# current CLI state. Separating this from execution allows tests to assert on
# the constructed pipeline without invoking ZFS.
zxfer_build_source_snapshot_list_cmd() {
	g_source_snapshot_list_uses_parallel=0
	g_source_snapshot_list_uses_metadata_compression=0
	l_local_serial_cmd=$(zxfer_render_zfs_command_for_spec "$g_LZFS" list -Hr -o name -s creation -t snapshot "$g_initial_source")

	if [ "$g_option_j_jobs" -le 1 ]; then
		printf '%s\n' "$l_local_serial_cmd"
		return
	fi

	if [ "$g_option_O_origin_host" = "" ]; then
		if zxfer_check_parallel_source_jobs_in_current_shell; then
			l_parallel_status=0
		else
			l_parallel_status=$?
		fi
		l_parallel_fallback_reason=$g_zxfer_parallel_source_job_check_result
		l_parallel_fallback_kind=$g_zxfer_parallel_source_job_check_kind
		if [ "$l_parallel_status" -ne 0 ]; then
			zxfer_note_parallel_source_discovery_fallback "$l_parallel_fallback_reason" local
			printf '%s\n' "$l_local_serial_cmd"
			return 0
		fi
	fi

	if ! l_parallel_threshold=$(zxfer_get_source_snapshot_parallel_dataset_threshold); then
		printf '%s\n' "Failed to determine the adaptive source snapshot discovery threshold."
		return 1
	fi

	if ! l_dataset_list=$(zxfer_get_source_snapshot_discovery_dataset_list); then
		printf '%s\n' "Failed to retrieve the source dataset list for adaptive snapshot discovery."
		return 1
	fi

	if ! l_dataset_count=$(zxfer_count_source_snapshot_discovery_datasets "$l_dataset_list"); then
		printf '%s\n' "Failed to count source datasets for adaptive snapshot discovery."
		return 1
	fi

	if [ "$l_dataset_count" -lt "$l_parallel_threshold" ]; then
		if [ "$g_option_O_origin_host" = "" ]; then
			printf '%s\n' "$l_local_serial_cmd"
			return 0
		fi

		zxfer_render_remote_source_snapshot_serial_list_cmd
		return 0
	fi

	if zxfer_check_parallel_source_jobs_in_current_shell; then
		l_parallel_status=0
	else
		l_parallel_status=$?
	fi
	l_parallel_fallback_reason=$g_zxfer_parallel_source_job_check_result
	l_parallel_fallback_kind=$g_zxfer_parallel_source_job_check_kind
	if [ "$l_parallel_status" -ne 0 ]; then
		if [ "$g_option_O_origin_host" = "" ]; then
			zxfer_note_parallel_source_discovery_fallback "$l_parallel_fallback_reason" local
			printf '%s\n' "$l_local_serial_cmd"
			return 0
		fi

		if zxfer_parallel_source_discovery_fallback_is_allowed \
			"$l_parallel_fallback_kind" origin; then
			zxfer_note_parallel_source_discovery_fallback "$l_parallel_fallback_reason" origin
			zxfer_render_remote_source_snapshot_serial_list_cmd
			return 0
		fi

		if [ -n "$l_parallel_fallback_reason" ]; then
			printf '%s\n' "$l_parallel_fallback_reason"
		else
			printf '%s\n' "Failed to prepare origin-host GNU parallel for adaptive source discovery."
		fi
		return "$l_parallel_status"
	fi

	g_source_snapshot_list_uses_parallel=1

	if [ ! "$g_option_O_origin_host" = "" ]; then
		l_parallel_path=$g_origin_parallel_cmd
		l_remote_zfs_cmd=${g_origin_cmd_zfs:-$g_cmd_zfs}
		l_parallel_cmd=$(zxfer_build_shell_command_from_argv "$l_parallel_path")
		l_remote_runner_cmd=$(zxfer_build_shell_command_from_argv \
			"$l_remote_zfs_cmd" list -H -o name -s creation -d 1 -t snapshot "{}")
		l_remote_parallel_cmd="$l_parallel_cmd -j $g_option_j_jobs --line-buffer -- \"$l_remote_runner_cmd\""
		if zxfer_should_inline_source_snapshot_dataset_list "$l_dataset_list" "$l_dataset_count"; then
			l_remote_dataset_input_cmd=$(zxfer_build_source_snapshot_dataset_list_printf_cmd "$l_dataset_list")
		else
			l_remote_dataset_input_cmd=$(zxfer_build_shell_command_from_argv \
				"$l_remote_zfs_cmd" list -Hr -t filesystem,volume -o name "$g_initial_source")
		fi
		l_remote_pipeline="$l_remote_dataset_input_cmd | $l_remote_parallel_cmd"
		if [ "$g_option_z_compress" -eq 1 ]; then
			g_source_snapshot_list_uses_metadata_compression=1
			l_remote_compress_safe=${g_origin_cmd_compress_safe:-$g_cmd_compress_safe}
			l_remote_pipeline="$l_remote_pipeline | $l_remote_compress_safe"
		fi
		l_remote_shell_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_pipeline")
		l_cmd=$(zxfer_build_ssh_shell_command_for_host "$g_option_O_origin_host" "$l_remote_shell_cmd") || return 1
		if [ "${g_source_snapshot_list_uses_metadata_compression:-0}" -eq 1 ]; then
			l_cmd="$l_cmd | $g_cmd_decompress_safe"
		fi
		printf '%s\n' "$l_cmd"
		return
	fi

	l_parallel_path=$g_cmd_parallel
	l_runner_cmd=$(zxfer_render_zfs_command_for_spec "$g_LZFS" list -H -o name -s creation -d 1 -t snapshot "{}")
	l_parallel_cmd="$(zxfer_build_shell_command_from_argv "$l_parallel_path") -j $g_option_j_jobs --line-buffer -- \"$l_runner_cmd\""
	if zxfer_should_inline_source_snapshot_dataset_list "$l_dataset_list" "$l_dataset_count"; then
		l_dataset_input_cmd=$(zxfer_build_source_snapshot_dataset_list_printf_cmd "$l_dataset_list")
	else
		l_dataset_input_cmd=$(zxfer_render_zfs_command_for_spec "$g_LZFS" list -Hr -t filesystem,volume -o name "$g_initial_source")
	fi
	l_cmd="$l_dataset_input_cmd | $l_parallel_cmd"
	printf '%s\n' "$l_cmd"
}

# Purpose: Render the source snapshot list preview command as a stable shell-
# safe or operator-facing string.
# Usage: Called during source and destination snapshot discovery when zxfer
# needs to display or transport the value without reparsing it.
#
# Dry-run snapshot discovery must stay render-only, so use the simple serial
# listing shape instead of the adaptive planner that probes datasets and
# validates optional helpers.
zxfer_render_source_snapshot_list_preview_cmd() {
	zxfer_render_zfs_command_for_spec "$g_LZFS" list -Hr -o name -s creation -t snapshot "$g_initial_source"
}

# Purpose: Write the source snapshot list to file in the normalized form later
# zxfer steps expect.
# Usage: Called during source and destination snapshot discovery when the
# module needs a stable staged file or emitted stream for downstream use.
#
# Determine the source snapshots sorted by creation time. Since this
# can take a long time, the command is run in the background. In addition,
# to optimize the process, gnu parallel is used to retrieve snapshots from
# multiple datasets concurrently.
zxfer_write_source_snapshot_list_to_file() {
	l_outfile=$1
	l_errfile=${2:-}
	l_cmd_tmp_file=""
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
		if ! l_cmd=$(zxfer_render_source_snapshot_list_preview_cmd); then
			zxfer_throw_error "${l_cmd:-Failed to render dry-run source snapshot discovery command.}"
		fi
		g_source_snapshot_list_cmd=$l_cmd
		zxfer_echoV "Dry run: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
		if zxfer_write_runtime_artifact_file "$l_outfile" ""; then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		if [ -n "$l_errfile" ]; then
			if zxfer_write_runtime_artifact_file "$l_errfile" ""; then
				:
			else
				l_status=$?
				return "$l_status"
			fi
		fi
		g_source_snapshot_list_pid=""
		return 0
	fi

	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_cmd_tmp_file=$g_zxfer_temp_file_result
	if ! zxfer_build_source_snapshot_list_cmd >"$l_cmd_tmp_file"; then
		if ! zxfer_read_snapshot_discovery_capture_file "$l_cmd_tmp_file"; then
			zxfer_cleanup_runtime_artifact_path "$l_cmd_tmp_file"
			zxfer_throw_error "Failed to read staged source snapshot discovery command after build failure."
		fi
		l_cmd=$g_zxfer_snapshot_discovery_file_read_result
		case "$l_cmd" in
		*'
')
			l_cmd=${l_cmd%?}
			;;
		esac
		zxfer_cleanup_runtime_artifact_path "$l_cmd_tmp_file"
		zxfer_throw_error "${l_cmd:-Failed to build source snapshot discovery command.}"
	fi
	if ! zxfer_read_snapshot_discovery_capture_file "$l_cmd_tmp_file"; then
		zxfer_cleanup_runtime_artifact_path "$l_cmd_tmp_file"
		zxfer_throw_error "Failed to read staged source snapshot discovery command."
	fi
	l_cmd=$g_zxfer_snapshot_discovery_file_read_result
	case "$l_cmd" in
	*'
')
		l_cmd=${l_cmd%?}
		;;
	esac
	zxfer_cleanup_runtime_artifact_path "$l_cmd_tmp_file"
	[ -n "$l_cmd" ] || zxfer_throw_error "Staged source snapshot discovery command was empty."
	g_source_snapshot_list_cmd=$l_cmd
	if [ "$g_option_O_origin_host" != "" ]; then
		zxfer_profile_record_ssh_invocation "$g_option_O_origin_host" source
	fi

	if [ "$g_option_j_jobs" -gt 1 ]; then
		if [ "${g_source_snapshot_list_uses_parallel:-0}" -eq 1 ]; then
			zxfer_profile_increment_counter g_zxfer_profile_source_snapshot_list_parallel_commands
		fi
		zxfer_echoV "Running command in the background: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
		if [ -n "$l_errfile" ]; then
			eval "$l_cmd" >"$l_outfile" 2>"$l_errfile" &
		else
			eval "$l_cmd" >"$l_outfile" &
		fi
		g_source_snapshot_list_pid=$!
		zxfer_register_cleanup_pid "$g_source_snapshot_list_pid"
	else
		zxfer_execute_background_cmd \
			"$l_cmd" \
			"$l_outfile" \
			"$l_errfile"
		g_source_snapshot_list_pid=${g_last_background_pid:-}
	fi
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
		l_cmd=$(zxfer_render_command_for_report "LC_ALL=C" sort "$l_input_file")
		zxfer_echoV "Running command: $l_cmd > $(zxfer_quote_token_for_report "$l_output_file")"
		zxfer_record_last_command_string "$l_cmd > $(zxfer_quote_token_for_report "$l_output_file")"
		LC_ALL=C sort "$l_input_file" >"$l_output_file"
	else
		l_escaped_destination_dataset=$(printf '%s\n' "$l_destination_dataset" | sed 's/[].[^$\\*|]/\\&/g')
		l_cmd=$(zxfer_render_command_for_report "" sed -e "s|$l_escaped_destination_dataset|$g_initial_source|g" "$l_input_file")
		zxfer_echoV "Running command: $l_cmd | LC_ALL=C sort > $(zxfer_quote_token_for_report "$l_output_file")"
		zxfer_record_last_command_string "$l_cmd | LC_ALL=C sort > $(zxfer_quote_token_for_report "$l_output_file")"
		sed -e "s|$l_escaped_destination_dataset|$g_initial_source|g" "$l_input_file" | LC_ALL=C sort >"$l_output_file"
	fi
}

# Purpose: Write the snapshot identity file from records in the normalized form
# later zxfer steps expect.
# Usage: Called during source and destination snapshot discovery when the
# module needs a stable staged file or emitted stream for downstream use.
zxfer_write_snapshot_identity_file_from_records() {
	l_snapshot_records=$1
	l_output_file=$2

	{
		while IFS= read -r l_snapshot_record; do
			[ -n "$l_snapshot_record" ] || continue
			l_snapshot_identity=$(zxfer_extract_snapshot_identity "$l_snapshot_record")
			[ -n "$l_snapshot_identity" ] || continue
			printf '%s\n' "$l_snapshot_identity"
		done <<-EOF
			$(zxfer_normalize_snapshot_record_list "$l_snapshot_records")
		EOF
	} | LC_ALL=C sort -u >"$l_output_file"
}

# Purpose: Refine the recursive snapshot deltas with identity validation with
# the additional validation this module owns.
# Usage: Called during source and destination snapshot discovery after an
# initial plan exists but before zxfer trusts it for live mutation.
zxfer_refine_recursive_snapshot_deltas_with_identity_validation() {
	l_source_sorted_file=$1
	l_destination_sorted_file=$2
	l_identity_source_datasets=""
	l_identity_destination_datasets=""
	l_identity_validation_error=""
	l_already_changed_datasets=""

	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_common_datasets_tmp_file=$g_zxfer_temp_file_result

	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_common_datasets_tmp_file"
		return "$l_status"
	fi
	l_already_changed_datasets_tmp_file=$g_zxfer_temp_file_result

	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file"
		return "$l_status"
	fi
	l_candidate_datasets_tmp_file=$g_zxfer_temp_file_result

	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" "$l_candidate_datasets_tmp_file"
		return "$l_status"
	fi
	l_common_snapshot_records_tmp_file=$g_zxfer_temp_file_result

	if ! LC_ALL=C comm -12 "$l_source_sorted_file" "$l_destination_sorted_file" >"$l_common_snapshot_records_tmp_file"; then
		zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
			"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
		zxfer_throw_error "Failed to derive recursive common snapshot list for snapshot identity validation."
	fi
	if ! zxfer_capture_recursive_dataset_list_from_snapshot_file "$l_common_snapshot_records_tmp_file"; then
		zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
			"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
		zxfer_throw_error "Failed to derive recursive common dataset list for snapshot identity validation."
	fi
	if ! zxfer_write_recursive_dataset_list_result_to_file "$l_common_datasets_tmp_file"; then
		zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
			"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
		zxfer_throw_error "Failed to stage recursive common dataset list for snapshot identity validation."
	fi

	if [ -n "${g_recursive_source_list:-}" ] || [ -n "${g_recursive_destination_extra_dataset_list:-}" ]; then
		l_already_changed_datasets=$g_recursive_source_list
		if [ -n "$g_recursive_destination_extra_dataset_list" ]; then
			if [ -n "$l_already_changed_datasets" ]; then
				l_already_changed_datasets=$l_already_changed_datasets'
'$g_recursive_destination_extra_dataset_list
			else
				l_already_changed_datasets=$g_recursive_destination_extra_dataset_list
			fi
		fi
		if ! zxfer_capture_recursive_dataset_list_from_snapshot_records "$l_already_changed_datasets"; then
			zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
				"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
			zxfer_throw_error "Failed to derive already-changed recursive dataset list for snapshot identity validation."
		fi
		if ! zxfer_write_recursive_dataset_list_result_to_file "$l_already_changed_datasets_tmp_file"; then
			zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
				"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
			zxfer_throw_error "Failed to stage already-changed recursive dataset list for snapshot identity validation."
		fi
	else
		if ! zxfer_write_runtime_artifact_file "$l_already_changed_datasets_tmp_file" ""; then
			zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
				"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
			zxfer_throw_error "Failed to stage empty already-changed recursive dataset list for snapshot identity validation."
		fi
	fi

	if ! LC_ALL=C comm -23 "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" >"$l_candidate_datasets_tmp_file"; then
		zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
			"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
		zxfer_throw_error "Failed to derive recursive candidate dataset list for snapshot identity validation."
	fi
	if [ "$g_option_x_exclude_datasets" != "" ]; then
		if ! zxfer_capture_recursive_dataset_list_from_lines_file "$l_candidate_datasets_tmp_file"; then
			zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
				"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
			zxfer_throw_error "Failed to load recursive candidate dataset list for snapshot identity validation filtering."
		fi
		if ! zxfer_filter_recursive_dataset_list_with_excludes "$g_zxfer_recursive_dataset_list_result"; then
			zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
				"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
			zxfer_throw_error "Failed to filter recursive candidate dataset list against exclude patterns during snapshot identity validation."
		fi
		if ! zxfer_write_recursive_dataset_list_result_to_file "$l_candidate_datasets_tmp_file"; then
			zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
				"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
			zxfer_throw_error "Failed to stage filtered recursive candidate dataset list for snapshot identity validation."
		fi
	fi

	if ! zxfer_read_snapshot_discovery_capture_file "$l_candidate_datasets_tmp_file"; then
		zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
			"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
		zxfer_throw_error "Failed to read staged recursive candidate dataset list for snapshot identity validation."
	fi

	while IFS= read -r l_candidate_dataset || [ -n "$l_candidate_dataset" ]; do
		[ -n "$l_candidate_dataset" ] || continue
		l_candidate_destination_dataset=$(zxfer_get_destination_dataset_for_source_dataset "$l_candidate_dataset")

		if ! l_source_identity_records=$(zxfer_get_snapshot_identity_records_for_dataset source "$l_candidate_dataset"); then
			l_identity_validation_error="Failed to retrieve source snapshot identities for [$l_candidate_dataset]."
			break
		fi

		if ! l_destination_identity_records=$(zxfer_get_snapshot_identity_records_for_dataset destination "$l_candidate_destination_dataset"); then
			l_identity_validation_error="Failed to retrieve destination snapshot identities for [$l_candidate_destination_dataset]."
			break
		fi

		if ! zxfer_get_temp_file >/dev/null; then
			l_identity_validation_error="Failed to allocate source snapshot identity staging for [$l_candidate_dataset]."
			break
		fi
		l_source_identity_tmp_file=$g_zxfer_temp_file_result

		if ! zxfer_get_temp_file >/dev/null; then
			zxfer_cleanup_runtime_artifact_path "$l_source_identity_tmp_file"
			l_identity_validation_error="Failed to allocate destination snapshot identity staging for [$l_candidate_destination_dataset]."
			break
		fi
		l_destination_identity_tmp_file=$g_zxfer_temp_file_result
		if ! zxfer_write_snapshot_identity_file_from_records "$l_source_identity_records" "$l_source_identity_tmp_file"; then
			zxfer_cleanup_runtime_artifact_paths "$l_source_identity_tmp_file" "$l_destination_identity_tmp_file"
			l_identity_validation_error="Failed to write source snapshot identities for [$l_candidate_dataset]."
			break
		fi
		if ! zxfer_write_snapshot_identity_file_from_records "$l_destination_identity_records" "$l_destination_identity_tmp_file"; then
			zxfer_cleanup_runtime_artifact_paths "$l_source_identity_tmp_file" "$l_destination_identity_tmp_file"
			l_identity_validation_error="Failed to write destination snapshot identities for [$l_candidate_destination_dataset]."
			break
		fi

		if ! l_source_identity_diff=$(zxfer_diff_snapshot_lists "$l_source_identity_tmp_file" "$l_destination_identity_tmp_file" "source_minus_destination"); then
			zxfer_cleanup_runtime_artifact_paths "$l_source_identity_tmp_file" "$l_destination_identity_tmp_file"
			l_identity_validation_error="Failed to diff source and destination snapshot identities for [$l_candidate_dataset]."
			break
		fi
		if [ -n "$l_source_identity_diff" ]; then
			if [ -n "$l_identity_source_datasets" ]; then
				l_identity_source_datasets="$l_identity_source_datasets
$l_candidate_dataset"
			else
				l_identity_source_datasets=$l_candidate_dataset
			fi
		fi

		if ! l_destination_identity_diff=$(zxfer_diff_snapshot_lists "$l_source_identity_tmp_file" "$l_destination_identity_tmp_file" "destination_minus_source"); then
			zxfer_cleanup_runtime_artifact_paths "$l_source_identity_tmp_file" "$l_destination_identity_tmp_file"
			l_identity_validation_error="Failed to diff destination and source snapshot identities for [$l_candidate_dataset]."
			break
		fi
		if [ -n "$l_destination_identity_diff" ]; then
			if [ -n "$l_identity_destination_datasets" ]; then
				l_identity_destination_datasets="$l_identity_destination_datasets
$l_candidate_dataset"
			else
				l_identity_destination_datasets=$l_candidate_dataset
			fi
		fi

		zxfer_cleanup_runtime_artifact_paths "$l_source_identity_tmp_file" "$l_destination_identity_tmp_file"
	done <<EOF
$g_zxfer_snapshot_discovery_file_read_result
EOF

	if [ -n "$l_identity_validation_error" ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
			"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
		zxfer_throw_error "$l_identity_validation_error"
	fi

	if [ -n "$l_identity_source_datasets" ]; then
		if ! zxfer_capture_recursive_dataset_list_from_snapshot_records "$(printf '%s\n%s\n' "$g_recursive_source_list" "$l_identity_source_datasets")"; then
			zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
				"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
			zxfer_throw_error "Failed to merge recursive source dataset list after snapshot identity validation."
		fi
		g_recursive_source_list=$g_zxfer_recursive_dataset_list_result
	fi

	if [ -n "$l_identity_destination_datasets" ]; then
		if ! zxfer_capture_recursive_dataset_list_from_snapshot_records "$(printf '%s\n%s\n' "$g_recursive_destination_extra_dataset_list" "$l_identity_destination_datasets")"; then
			zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
				"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
			zxfer_throw_error "Failed to merge recursive destination dataset list after snapshot identity validation."
		fi
		g_recursive_destination_extra_dataset_list=$g_zxfer_recursive_dataset_list_result
	fi

	zxfer_cleanup_runtime_artifact_paths "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" \
		"$l_candidate_datasets_tmp_file" "$l_common_snapshot_records_tmp_file"
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

	# determine the last dataset in $g_initial_source. This will be the last
	# dataset after a forward slash "/" or if no forward slash exists, then
	# is is the name of the dataset itself.
	l_source_dataset=$(echo "$g_initial_source" | awk -F'/' '{print $NF}')

	if [ "$g_initial_source_had_trailing_slash" -eq 1 ]; then
		# Trailing slash replicates directly into $g_destination (no child dataset)
		l_destination_dataset="$g_destination"
	else
		l_destination_dataset="$g_destination/$l_source_dataset"
	fi

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		if ! l_cmd=$(zxfer_render_destination_zfs_command list -Hr -o name -t snapshot "$l_destination_dataset"); then
			zxfer_throw_error "${l_cmd:-Failed to render dry-run destination snapshot discovery command.}"
		fi
		zxfer_echoV "Dry run: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
		if zxfer_write_runtime_artifact_file "$l_rzfs_list_hr_snap_tmp_file" ""; then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		if zxfer_write_runtime_artifact_file "$l_dest_snaps_stripped_sorted_tmp_file" ""; then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		return
	fi

	# check if the destination zfs dataset exists before listing snapshots
	if ! l_destination_exists=$(zxfer_exists_destination "$l_destination_dataset"); then
		zxfer_throw_error "$l_destination_exists"
	fi

	if [ "$l_destination_exists" -eq 1 ]; then
		# dataset exists
		# Keep destination-side snapshot listing serial here. The older parallel
		# variant added complexity and was not a net win once metadata was cached.
		l_cmd=$(zxfer_render_destination_zfs_command list -Hr -o name -t snapshot "$l_destination_dataset")
		zxfer_echoV "Running command: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
		# make sure to eval and then pipe the contents to the file in case
		# the command uses ssh
		if ! zxfer_run_destination_zfs_cmd list -Hr -o name -t snapshot "$l_destination_dataset" >"$l_rzfs_list_hr_snap_tmp_file"; then
			zxfer_throw_error "Failed to retrieve snapshot list from the destination."
		fi

	else
		# dataset does not exist
		zxfer_echoV "Destination dataset does not exist: $l_destination_dataset"
		if ! zxfer_write_runtime_artifact_file "$l_rzfs_list_hr_snap_tmp_file" ""; then
			zxfer_throw_error "Failed to stage empty destination snapshot list."
		fi
	fi

	zxfer_normalize_destination_snapshot_list "$l_destination_dataset" "$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
}

# Purpose: Diff the snapshot lists so later helpers act on exact deltas.
# Usage: Called during source and destination snapshot discovery before
# reconciliation or apply logic mutates live state from the computed
# difference.
#
# compare the source and destination snapshots and identify source datasets
# that are not in the destination. Set g_recursive_source_list to the
# datasets that contain snapshots that are not in the destination.
# Afterwards, g_recursive_source_list only contains the names of
# the datasets that need to be transferred.
#
# Diff sorted snapshot listings. The default mode returns snapshots only
# present in the source, while destination_minus_source highlights extra
# snapshots on the target.
zxfer_diff_snapshot_lists() {
	l_source_sorted_file=$1
	l_destination_sorted_file=$2
	l_mode=${3:-source_minus_destination}

	case "$l_mode" in
	source_minus_destination)
		LC_ALL=C comm -23 "$l_source_sorted_file" "$l_destination_sorted_file"
		;;
	destination_minus_source)
		LC_ALL=C comm -13 "$l_source_sorted_file" "$l_destination_sorted_file"
		;;
	*)
		zxfer_throw_error "Unknown snapshot diff mode: $l_mode"
		;;
	esac
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

	l_line_count=$(wc -l <"$l_input_file" 2>/dev/null | tr -d '[:space:]') || return 1
	case "$l_line_count" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	[ "$l_line_count" -le "$l_max_lines" ]
}

# Purpose: Reverse the numbered file lines with awk while preserving the record
# structure later helpers rely on.
# Usage: Called during source and destination snapshot discovery when
# comparison or replay logic needs the same data in the opposite order.
zxfer_reverse_numbered_file_lines_with_awk() {
	l_input_file=$1

	# shellcheck disable=SC2016  # awk program should see literal $0/NR.
	"${g_cmd_awk:-awk}" '{ l_line = $0; l_tab = index(l_line, "\t"); if (l_tab > 0) l_line = substr(l_line, l_tab + 1); l_lines[NR] = l_line } END { for (l_i = NR; l_i >= 1; l_i--) print l_lines[l_i] }' "$l_input_file"
}

# Purpose: Check whether the numbered file is strictly increasing.
# Usage: Called during source and destination snapshot discovery when later
# helpers need a boolean answer about the numbered file.
zxfer_numbered_file_is_strictly_increasing() {
	l_input_file=$1

	# shellcheck disable=SC2016  # awk program should see literal $0/NR.
	"${g_cmd_awk:-awk}" 'BEGIN { l_prev = -1 } { l_tab = index($0, "\t"); if (l_tab <= 1) exit 1; l_number = substr($0, 1, l_tab - 1); gsub(/^[[:space:]]+/, "", l_number); gsub(/[[:space:]]+$/, "", l_number); if (l_number == "" || l_number ~ /[^0-9]/) exit 1; if (NR > 1 && (l_number + 0) <= l_prev) exit 1; l_prev = l_number + 0 }' "$l_input_file"
}

# Purpose: Reverse the numbered file lines with sort while preserving the
# record structure later helpers rely on.
# Usage: Called during source and destination snapshot discovery when
# comparison or replay logic needs the same data in the opposite order.
zxfer_reverse_numbered_file_lines_with_sort() {
	l_input_file=$1

	LC_ALL=C sort -nr "$l_input_file" | cut -f2-
}

# Purpose: Reverse the numbered line stream while preserving the record
# structure later helpers rely on.
# Usage: Called during source and destination snapshot discovery when
# comparison or replay logic needs the same data in the opposite order.
#
# Reverse a numbered line stream produced by `cat -n`. Strip the line number by
# tab-delimited field rather than a fixed character offset so large line counts
# do not truncate the first character of the payload.
zxfer_reverse_numbered_line_stream() {
	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_input_tmp_file=$g_zxfer_temp_file_result
	cat >"$l_input_tmp_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_input_tmp_file"
		return "$l_status"
	}

	if zxfer_should_use_linear_reverse_for_file "$l_input_tmp_file" &&
		zxfer_numbered_file_is_strictly_increasing "$l_input_tmp_file"; then
		zxfer_reverse_numbered_file_lines_with_awk "$l_input_tmp_file"
	else
		zxfer_reverse_numbered_file_lines_with_sort "$l_input_tmp_file"
	fi
	l_status=$?
	zxfer_cleanup_runtime_artifact_path "$l_input_tmp_file"
	return "$l_status"
}

# Purpose: Reverse the plain file lines with awk while preserving the record
# structure later helpers rely on.
# Usage: Called during source and destination snapshot discovery when
# comparison or replay logic needs the same data in the opposite order.
zxfer_reverse_plain_file_lines_with_awk() {
	l_input_file=$1

	# shellcheck disable=SC2016  # awk program should see literal $0/NR.
	"${g_cmd_awk:-awk}" '{ l_lines[NR] = $0 } END { for (l_i = NR; l_i >= 1; l_i--) print l_lines[l_i] }' "$l_input_file"
}

# Purpose: Reverse the plain file lines with sort while preserving the record
# structure later helpers rely on.
# Usage: Called during source and destination snapshot discovery when
# comparison or replay logic needs the same data in the opposite order.
zxfer_reverse_plain_file_lines_with_sort() {
	l_input_file=$1
	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_numbered_tmp_file=$g_zxfer_temp_file_result

	if cat -n "$l_input_file" >"$l_numbered_tmp_file"; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_numbered_tmp_file"
		return "$l_status"
	fi

	zxfer_reverse_numbered_file_lines_with_sort "$l_numbered_tmp_file"
	l_status=$?
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
		zxfer_reverse_plain_file_lines_with_awk "$l_input_file"
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

	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_dataset_list_sorted_file=$g_zxfer_temp_file_result

	if LC_ALL=C sort -u "$l_dataset_lines_file" >"$l_dataset_list_sorted_file"; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dataset_list_sorted_file"
		return "$l_status"
	fi

	if zxfer_read_snapshot_discovery_capture_file "$l_dataset_list_sorted_file"; then
		:
	else
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dataset_list_sorted_file"
		return "$l_read_status"
	fi

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

	zxfer_cleanup_runtime_artifact_path "$l_dataset_list_sorted_file"
	return 0
}

# Purpose: Capture the recursive dataset list from snapshot records into staged
# state or module globals for later use.
# Usage: Called during source and destination snapshot discovery when later
# helpers need a checked snapshot of command output or computed state.
zxfer_capture_recursive_dataset_list_from_snapshot_records() {
	l_snapshot_records=$1

	g_zxfer_recursive_dataset_list_result=""
	[ -n "$l_snapshot_records" ] || return 0

	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_snapshot_records_file=$g_zxfer_temp_file_result
	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_snapshot_records_file"
		return "$l_status"
	fi
	l_dataset_lines_file=$g_zxfer_temp_file_result

	if zxfer_write_runtime_artifact_file "$l_snapshot_records_file" "$l_snapshot_records
"; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_snapshot_records_file"
		zxfer_cleanup_runtime_artifact_path "$l_dataset_lines_file"
		return "$l_status"
	fi

	# shellcheck disable=SC2016  # awk script should see literal $1.
	if "$g_cmd_awk" -F@ '{print $1}' "$l_snapshot_records_file" >"$l_dataset_lines_file"; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_snapshot_records_file"
		zxfer_cleanup_runtime_artifact_path "$l_dataset_lines_file"
		return "$l_status"
	fi

	if zxfer_capture_recursive_dataset_list_from_lines_file "$l_dataset_lines_file"; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_snapshot_records_file"
		zxfer_cleanup_runtime_artifact_path "$l_dataset_lines_file"
		return "$l_status"
	fi

	zxfer_cleanup_runtime_artifact_path "$l_snapshot_records_file"
	zxfer_cleanup_runtime_artifact_path "$l_dataset_lines_file"
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

	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_dataset_lines_file=$g_zxfer_temp_file_result

	# shellcheck disable=SC2016  # awk script should see literal $1.
	if "$g_cmd_awk" -F@ '{print $1}' "$l_snapshot_records_file" >"$l_dataset_lines_file"; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dataset_lines_file"
		return "$l_status"
	fi

	if zxfer_capture_recursive_dataset_list_from_lines_file "$l_dataset_lines_file"; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dataset_lines_file"
		return "$l_status"
	fi

	zxfer_cleanup_runtime_artifact_path "$l_dataset_lines_file"
	return 0
}

# Purpose: Write the recursive dataset list result to file in the normalized
# form later zxfer steps expect.
# Usage: Called during source and destination snapshot discovery when the
# module needs a stable staged file or emitted stream for downstream use.
zxfer_write_recursive_dataset_list_result_to_file() {
	l_output_file=$1

	if [ -n "$g_zxfer_recursive_dataset_list_result" ]; then
		zxfer_write_runtime_artifact_file "$l_output_file" "$g_zxfer_recursive_dataset_list_result
"
	else
		zxfer_write_runtime_artifact_file "$l_output_file" ""
	fi
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

	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_dataset_list_input_file=$g_zxfer_temp_file_result
	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dataset_list_input_file"
		return "$l_status"
	fi
	l_dataset_list_filtered_file=$g_zxfer_temp_file_result

	if zxfer_write_runtime_artifact_file "$l_dataset_list_input_file" "$l_dataset_list
"; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dataset_list_input_file"
		zxfer_cleanup_runtime_artifact_path "$l_dataset_list_filtered_file"
		return "$l_status"
	fi

	grep -v -e "$g_option_x_exclude_datasets" "$l_dataset_list_input_file" >"$l_dataset_list_filtered_file"
	l_filter_status=$?
	case "$l_filter_status" in
	0 | 1) ;;
	*)
		zxfer_cleanup_runtime_artifact_path "$l_dataset_list_input_file"
		zxfer_cleanup_runtime_artifact_path "$l_dataset_list_filtered_file"
		return "$l_filter_status"
		;;
	esac

	if zxfer_read_snapshot_discovery_capture_file "$l_dataset_list_filtered_file"; then
		:
	else
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_dataset_list_input_file"
		zxfer_cleanup_runtime_artifact_path "$l_dataset_list_filtered_file"
		return "$l_read_status"
	fi

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

	zxfer_cleanup_runtime_artifact_path "$l_dataset_list_input_file"
	zxfer_cleanup_runtime_artifact_path "$l_dataset_list_filtered_file"
	return 0
}

# Purpose: Update the g recursive source list in the shared runtime state.
# Usage: Called during source and destination snapshot discovery after a probe
# or planning step changes the active context that later helpers should use.
zxfer_set_g_recursive_source_list() {
	l_lzfs_list_hr_s_snap_tmp_file=$1
	l_dest_snaps_stripped_sorted_tmp_file=$2

	zxfer_get_temp_file >/dev/null
	l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_source_snaps_sorted_tmp_file=$g_zxfer_temp_file_result

	# sort the source snapshots for use with comm
	# wait until background processes are finished before attempting to sort
	l_cmd=$(zxfer_render_command_for_report "LC_ALL=C" sort "$l_lzfs_list_hr_s_snap_tmp_file")
	zxfer_echoV "Running command: $l_cmd > $(zxfer_quote_token_for_report "$l_source_snaps_sorted_tmp_file")"
	zxfer_record_last_command_string "$l_cmd > $(zxfer_quote_token_for_report "$l_source_snaps_sorted_tmp_file")"
	if ! LC_ALL=C sort "$l_lzfs_list_hr_s_snap_tmp_file" >"$l_source_snaps_sorted_tmp_file"; then
		zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
		zxfer_throw_error "Failed to sort source snapshots for recursive delta planning."
	fi

	if ! l_missing_snapshots=$(zxfer_diff_snapshot_lists "$l_source_snaps_sorted_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" "source_minus_destination"); then
		zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
		zxfer_throw_error "Failed to diff source and destination snapshots for recursive transfer planning."
	fi
	if ! l_destination_extra_snapshots=$(zxfer_diff_snapshot_lists "$l_source_snaps_sorted_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" "destination_minus_source"); then
		zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
		zxfer_throw_error "Failed to diff destination and source snapshots for recursive delete planning."
	fi
	if [ "$l_missing_snapshots" != "" ]; then
		if ! zxfer_capture_recursive_dataset_list_from_snapshot_records "$l_missing_snapshots"; then
			zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
			zxfer_throw_error "Failed to derive recursive source dataset transfer list."
		fi
		g_recursive_source_list=$g_zxfer_recursive_dataset_list_result
	else
		g_recursive_source_list=""
	fi
	if [ "$l_destination_extra_snapshots" != "" ]; then
		if ! zxfer_capture_recursive_dataset_list_from_snapshot_records "$l_destination_extra_snapshots"; then
			zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
			zxfer_throw_error "Failed to derive recursive destination dataset delete list."
		fi
		g_recursive_destination_extra_dataset_list=$g_zxfer_recursive_dataset_list_result
	else
		g_recursive_destination_extra_dataset_list=""
	fi
	if ! zxfer_capture_recursive_dataset_list_from_snapshot_file "$l_source_snaps_sorted_tmp_file"; then
		zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
		zxfer_throw_error "Failed to derive recursive source dataset inventory."
	fi
	g_recursive_source_dataset_list=$g_zxfer_recursive_dataset_list_result

	# if excluding datasets, remove them from the list
	if [ "$g_option_x_exclude_datasets" != "" ]; then
		if ! zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_source_list"; then
			zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
			zxfer_throw_error "Failed to filter recursive source dataset transfer list against exclude patterns."
		fi
		g_recursive_source_list=$g_zxfer_recursive_dataset_list_result
		if ! zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_destination_extra_dataset_list"; then
			zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
			zxfer_throw_error "Failed to filter recursive destination dataset delete list against exclude patterns."
		fi
		g_recursive_destination_extra_dataset_list=$g_zxfer_recursive_dataset_list_result
		if ! zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_source_dataset_list"; then
			zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
			zxfer_throw_error "Failed to filter recursive source dataset inventory against exclude patterns."
		fi
		g_recursive_source_dataset_list=$g_zxfer_recursive_dataset_list_result
	fi

	zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$l_source_snaps_sorted_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
	l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
		return "$l_status"
	fi

	# debugging
	if [ "$g_option_V_very_verbose" -eq 1 ]; then
		echo "====================================================================="
		echo "====== Snapshots present in source but missing in destination ======"
		if [ "$l_missing_snapshots" != "" ]; then
			printf '%s\n' "$l_missing_snapshots"
		fi
		echo "====== Source datasets that differ from destination ======"
		echo "g_recursive_source_list:"
		echo "$g_recursive_source_list"
		echo "Source dataset count: $(echo "$g_recursive_source_list" | grep -cve '^\s*$')"
		echo "====================================================================="
		echo "====== Extra Destination snapshots not in source ======"
		if [ "$l_destination_extra_snapshots" != "" ]; then
			printf '%s\n' "$l_destination_extra_snapshots"
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

	zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
}

# Purpose: Build the source and destination snapshot inventories that the rest
# of replication planning depends on.
# Usage: Called during source and destination snapshot discovery near the start
# of each live pass so later delete, seed, and send decisions work from one
# shared discovery result.
#
# Build the source and destination snapshot caches used by replication.
# zxfer relies on `zfs list` in machine-readable mode (`-H`), recursive dataset
# traversal (`-r`) where needed, name-only output during initial discovery
# (`-o name`),
# snapshot-only listing (`-t snapshot`), and creation-order sorting for
# per-dataset snapshot discovery on the source side. Guid-bearing identity
# records are fetched lazily later only for datasets that still need exact
# common-snapshot or delete validation.
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

	# create temporary files used by the background processes
	zxfer_get_temp_file >/dev/null
	l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_lzfs_list_hr_s_snap_tmp_file=$g_zxfer_temp_file_result
	zxfer_get_temp_file >/dev/null
	l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_lzfs_list_hr_s_snap_tmp_file"
		return "$l_status"
	fi
	l_lzfs_list_hr_s_snap_err_tmp_file=$g_zxfer_temp_file_result

	#
	# BEGIN background process
	#
	g_source_snapshot_list_pid=""
	l_source_snapshot_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	zxfer_write_source_snapshot_list_to_file "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file"

	#
	# Run as many commands prior to the wait command as possible.
	#

	# get a list of all destination datasets recursively
	l_destination_snapshot_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	l_cmd=$(zxfer_render_destination_zfs_command list -t filesystem,volume -Hr -o name "$g_destination")
	zxfer_echoV "Running command: $l_cmd"
	zxfer_record_last_command_string "$l_cmd"
	zxfer_get_temp_file >/dev/null
	l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file"
		return "$l_status"
	fi
	l_dest_list_tmp_file=$g_zxfer_temp_file_result
	zxfer_get_temp_file >/dev/null
	l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
			"$l_dest_list_tmp_file"
		return "$l_status"
	fi
	l_dest_list_err_file=$g_zxfer_temp_file_result
	if zxfer_run_destination_zfs_cmd list -t filesystem,volume -Hr -o name "$g_destination" >"$l_dest_list_tmp_file" 2>"$l_dest_list_err_file"; then
		if ! zxfer_read_snapshot_discovery_capture_file "$l_dest_list_tmp_file"; then
			zxfer_cleanup_runtime_artifact_paths "$l_dest_list_tmp_file" "$l_dest_list_err_file"
			zxfer_throw_usage_error "Failed to read staged destination dataset inventory."
		fi
		g_recursive_dest_list=$g_zxfer_snapshot_discovery_file_read_result
		[ -n "$g_recursive_dest_list" ] || {
			zxfer_cleanup_runtime_artifact_paths "$l_dest_list_tmp_file" "$l_dest_list_err_file"
			zxfer_throw_usage_error "Staged destination dataset inventory was empty."
		}
		zxfer_seed_destination_existence_cache_from_recursive_list "$g_destination" "$g_recursive_dest_list"
	else
		if zxfer_read_snapshot_discovery_capture_file "$l_dest_list_err_file"; then
			l_dest_err=$g_zxfer_snapshot_discovery_file_read_result
		else
			zxfer_cleanup_runtime_artifact_paths "$l_dest_list_tmp_file" "$l_dest_list_err_file"
			zxfer_throw_usage_error "Failed to read staged destination dataset inventory stderr."
		fi
		if zxfer_destination_probe_reports_missing "$l_dest_err"; then
			l_dest_pool=${g_destination%%/*}
			if [ "$l_dest_pool" = "" ]; then
				l_dest_pool=$g_destination
			fi
			if zxfer_run_destination_zfs_cmd list -H -o name "$l_dest_pool" >/dev/null 2>&1; then
				g_recursive_dest_list=""
				zxfer_mark_destination_root_missing_in_cache "$g_destination"
				zxfer_echoV "Destination dataset missing; treating as empty list for bootstrap."
			else
				zxfer_cleanup_runtime_artifact_paths "$l_dest_list_tmp_file" "$l_dest_list_err_file"
				zxfer_throw_usage_error "Failed to retrieve list of datasets from the destination"
			fi
		else
			zxfer_cleanup_runtime_artifact_paths "$l_dest_list_tmp_file" "$l_dest_list_err_file"
			zxfer_throw_usage_error "Failed to retrieve list of datasets from the destination"
		fi
	fi
	zxfer_cleanup_runtime_artifact_paths "$l_dest_list_tmp_file" "$l_dest_list_err_file"

	zxfer_get_temp_file >/dev/null
	l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file"
		return "$l_status"
	fi
	l_rzfs_list_hr_snap_tmp_file=$g_zxfer_temp_file_result
	zxfer_get_temp_file >/dev/null
	l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
			"$l_rzfs_list_hr_snap_tmp_file"
		return "$l_status"
	fi
	l_dest_snaps_stripped_sorted_tmp_file=$g_zxfer_temp_file_result

	# this function writes to both files passed as parameters
	zxfer_write_destination_snapshot_list_to_files "$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
	zxfer_profile_add_elapsed_ms g_zxfer_profile_destination_snapshot_listing_ms "$l_destination_snapshot_stage_start_ms"

	if ! zxfer_read_snapshot_discovery_capture_file "$l_rzfs_list_hr_snap_tmp_file"; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
			"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
		zxfer_throw_error "Failed to read staged destination snapshot list."
	fi
	g_rzfs_list_hr_snap=$g_zxfer_snapshot_discovery_file_read_result
	g_zxfer_destination_snapshot_record_cache_file=$l_rzfs_list_hr_snap_tmp_file

	zxfer_echoV "Waiting for background processes to finish."
	l_source_snapshot_wait_status=0
	if [ -n "${g_source_snapshot_list_pid:-}" ]; then
		wait "$g_source_snapshot_list_pid" || l_source_snapshot_wait_status=$?
		zxfer_unregister_cleanup_pid "$g_source_snapshot_list_pid"
		g_source_snapshot_list_pid=""
	fi
	zxfer_profile_add_elapsed_ms g_zxfer_profile_source_snapshot_listing_ms "$l_source_snapshot_stage_start_ms"

	if [ "$l_source_snapshot_wait_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
			"$l_dest_snaps_stripped_sorted_tmp_file"
		zxfer_cleanup_snapshot_record_cache_files
		if [ -n "${g_source_snapshot_list_cmd:-}" ]; then
			zxfer_record_last_command_string "$g_source_snapshot_list_cmd"
		fi
		if zxfer_read_snapshot_discovery_capture_file "$l_lzfs_list_hr_s_snap_err_tmp_file"; then
			l_source_snapshot_err=$g_zxfer_snapshot_discovery_file_read_result
			l_source_snapshot_err=$(zxfer_limit_snapshot_discovery_capture_lines \
				"$l_source_snapshot_err" 10)
		else
			zxfer_cleanup_runtime_artifact_path "$l_lzfs_list_hr_s_snap_err_tmp_file"
			zxfer_throw_error "Failed to read staged source snapshot stderr."
		fi
		zxfer_cleanup_runtime_artifact_path "$l_lzfs_list_hr_s_snap_err_tmp_file"
		if [ "$l_source_snapshot_err" != "" ]; then
			zxfer_throw_error "Failed to retrieve snapshots from the source: $l_source_snapshot_err" 3
		fi
		zxfer_throw_error "Failed to retrieve snapshots from the source" 3
	fi
	if ! zxfer_read_snapshot_discovery_capture_file "$l_lzfs_list_hr_s_snap_tmp_file"; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
			"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
		zxfer_cleanup_snapshot_record_cache_files
		zxfer_throw_error "Failed to read staged source snapshot list."
	fi
	g_lzfs_list_hr_snap=$g_zxfer_snapshot_discovery_file_read_result
	zxfer_get_temp_file >/dev/null
	l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
			"$l_lzfs_list_hr_s_snap_err_tmp_file" \
			"$l_dest_snaps_stripped_sorted_tmp_file"
		zxfer_cleanup_snapshot_record_cache_files
		return "$l_status"
	fi
	l_source_snapshot_record_cache_file=$g_zxfer_temp_file_result
	l_cmd=$(zxfer_render_command_for_report "" zxfer_reverse_file_lines "$l_lzfs_list_hr_s_snap_tmp_file")
	zxfer_echoV "Running command: $l_cmd > $(zxfer_quote_token_for_report "$l_source_snapshot_record_cache_file")"
	zxfer_record_last_command_string "$l_cmd > $(zxfer_quote_token_for_report "$l_source_snapshot_record_cache_file")"
	if ! zxfer_reverse_file_lines "$l_lzfs_list_hr_s_snap_tmp_file" >"$l_source_snapshot_record_cache_file"; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
			"$l_lzfs_list_hr_s_snap_err_tmp_file" \
			"$l_dest_snaps_stripped_sorted_tmp_file" \
			"$l_source_snapshot_record_cache_file"
		zxfer_cleanup_snapshot_record_cache_files
		zxfer_throw_error "Failed to stage source snapshot record cache."
	fi
	g_zxfer_source_snapshot_record_cache_file=$l_source_snapshot_record_cache_file
	zxfer_echoV "Background processes finished."

	#
	# END background process
	#
	l_snapshot_diff_sort_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	zxfer_set_g_recursive_source_list "$l_lzfs_list_hr_s_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
	l_status=$?
	zxfer_profile_add_elapsed_ms g_zxfer_profile_snapshot_diff_sort_ms "$l_snapshot_diff_sort_stage_start_ms"
	zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
		"$l_dest_snaps_stripped_sorted_tmp_file"
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_lzfs_list_hr_s_snap_err_tmp_file"
		zxfer_cleanup_snapshot_record_cache_files
		return "$l_status"
	fi
	zxfer_cleanup_runtime_artifact_path "$l_lzfs_list_hr_s_snap_err_tmp_file"

	#
	# Errors
	#

	if [ "$g_lzfs_list_hr_snap" = "" ]; then
		zxfer_throw_error "Failed to retrieve snapshots from the source" 3
	fi

	if [ "$g_recursive_dest_list" = "" ]; then
		zxfer_echoV "Destination dataset list is empty; assuming no existing datasets under \"$g_destination\""
	fi

	zxfer_echoV "End zxfer_get_zfs_list()"
}
