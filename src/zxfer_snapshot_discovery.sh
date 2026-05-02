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
	g_source_snapshot_list_job_id=""
	g_source_snapshot_list_uses_parallel=0
	g_source_snapshot_list_uses_metadata_compression=0
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_recursive_dest_list=""
	g_recursive_destination_extra_dataset_list=""
	g_zxfer_snapshot_discovery_file_read_result=""
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
	if zxfer_read_runtime_artifact_file "$l_capture_path" >/dev/null; then
		g_zxfer_snapshot_discovery_file_read_result=$g_zxfer_runtime_artifact_read_result
	else
		l_read_status=$?
		return "$l_read_status"
	fi

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

	if zxfer_ensure_parallel_available_for_source_jobs >/dev/null 2>&1; then
		l_status=0
	else
		l_status=$?
	fi

	return "$l_status"
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
	if l_local_serial_cmd=$(zxfer_render_zfs_command_for_spec "$g_LZFS" list -Hr -o name,guid -s creation -t snapshot "$g_initial_source"); then
		:
	else
		l_status=$?
		return "$l_status"
	fi

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
		if l_parallel_cmd=$(zxfer_build_shell_command_from_argv "$l_parallel_path"); then
			:
		else
			l_status=$?
			return "$l_status"
		fi
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
		l_remote_pipeline="$l_remote_dataset_input_cmd | $l_remote_parallel_cmd"
		if [ "$g_option_z_compress" -eq 1 ]; then
			g_source_snapshot_list_uses_metadata_compression=1
			l_remote_compress_safe=${g_origin_cmd_compress_safe:-$g_cmd_compress_safe}
			l_remote_pipeline="$l_remote_pipeline | $l_remote_compress_safe"
		fi
		if l_remote_shell_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_pipeline"); then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		if l_cmd=$(zxfer_build_ssh_shell_command_for_host "$g_option_O_origin_host" "$l_remote_shell_cmd"); then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		if [ "${g_source_snapshot_list_uses_metadata_compression:-0}" -eq 1 ]; then
			l_cmd="$l_cmd | $g_cmd_decompress_safe"
		fi
		printf '%s\n' "$l_cmd"
		return
	fi

	l_parallel_path=$g_cmd_parallel
	if l_runner_cmd=$(zxfer_render_zfs_command_for_spec "$g_LZFS" list -H -o name,guid -s creation -d 1 -t snapshot "{}"); then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	if l_parallel_cmd=$(zxfer_build_shell_command_from_argv "$l_parallel_path"); then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_parallel_cmd="$l_parallel_cmd -j $g_option_j_jobs --line-buffer -- \"$l_runner_cmd\""
	if l_dataset_input_cmd=$(zxfer_render_zfs_command_for_spec \
		"$g_LZFS" list -Hr -t filesystem,volume -o name "$g_initial_source"); then
		:
	else
		l_status=$?
		return "$l_status"
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
# listing shape instead of resolving parallel helpers during preview-only runs.
zxfer_render_source_snapshot_list_preview_cmd() {
	zxfer_render_zfs_command_for_spec "$g_LZFS" list -Hr -o name,guid -s creation -t snapshot "$g_initial_source"
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
		l_status=0
		l_cmd=$(zxfer_render_source_snapshot_list_preview_cmd) || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_throw_error "${l_cmd:-Failed to render dry-run source snapshot discovery command.}" "$l_status"
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
		g_source_snapshot_list_job_id=""
		return 0
	fi

	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_cmd_tmp_file=$g_zxfer_temp_file_result
	l_status=0
	zxfer_build_source_snapshot_list_cmd >"$l_cmd_tmp_file" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		l_read_status=0
		zxfer_read_source_snapshot_discovery_command_file "$l_cmd_tmp_file" || l_read_status=$?
		if [ "$l_read_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path "$l_cmd_tmp_file"
			zxfer_throw_error "Failed to read staged source snapshot discovery command after build failure." "$l_read_status"
		fi
		l_cmd=$g_zxfer_snapshot_discovery_file_read_result
		zxfer_cleanup_runtime_artifact_path "$l_cmd_tmp_file"
		zxfer_throw_error "${l_cmd:-Failed to build source snapshot discovery command.}" "$l_status"
	fi
	l_status=0
	zxfer_read_source_snapshot_discovery_command_file "$l_cmd_tmp_file" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_cmd_tmp_file"
		zxfer_throw_error "Failed to read staged source snapshot discovery command." "$l_status"
	fi
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
	if zxfer_execute_background_cmd "$l_cmd" "$l_outfile" "$l_errfile"; then
		:
	else
		l_status=$?
		return "$l_status"
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
		l_cmd=$(zxfer_render_command_for_report "LC_ALL=C" sort "$l_input_file")
		zxfer_echoV "Running command: $l_cmd > $(zxfer_quote_token_for_report "$l_output_file")"
		zxfer_record_last_command_string "$l_cmd > $(zxfer_quote_token_for_report "$l_output_file")"
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
		l_cmd=$(zxfer_render_command_for_report "" "${g_cmd_awk:-awk}" \
			-v "destination_dataset=$l_destination_dataset" \
			-v "initial_source=$g_initial_source" \
			"$l_prefix_rewrite_program" \
			"$l_input_file")
		zxfer_echoV "Running command: $l_cmd > $(zxfer_quote_token_for_report "$l_output_file")"
		zxfer_record_last_command_string "$l_cmd > $(zxfer_quote_token_for_report "$l_output_file")"
		"${g_cmd_awk:-awk}" \
			-v "destination_dataset=$l_destination_dataset" \
			-v "initial_source=$g_initial_source" \
			"$l_prefix_rewrite_program" \
			"$l_input_file" >"$l_output_file" ||
			return "$?"
		l_cmd=$(zxfer_render_command_for_report "LC_ALL=C" sort -o "$l_output_file" "$l_output_file")
		zxfer_echoV "Running command: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
		LC_ALL=C sort -o "$l_output_file" "$l_output_file"
	fi
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

	l_status=0
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
			if (index($0, "STATUS" tab) == 1) {
				if (current_section != "") {
					fail()
				}
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
				section_name = substr($0, 5)
				if (current_section == "" || section_name != current_section) {
					fail()
				}
				current_section = ""
				current_output = ""
				next
			}
			if (current_section == "") {
				fail()
			}
			append_section_line($0)
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
	' || l_status=$?
	return "$l_status"
}

# Purpose: Parse target-side destination discovery output into staged files and
# compact status state.
# Usage: Called by tests and fallback file-backed paths; live remote discovery
# uses the streaming splitter directly to avoid a full local post-processing
# pass over large snapshot lists.
zxfer_parse_remote_destination_discovery_batch_output_file() {
	l_batch_output_file=$1
	l_dest_list_tmp_file=$2
	l_dest_list_err_file=$3
	l_rzfs_list_hr_snap_tmp_file=$4
	l_rzfs_list_hr_snap_err_tmp_file=$5
	l_batch_status_file=""

	zxfer_reset_destination_discovery_batch_state

	l_status=0
	zxfer_get_temp_file >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_batch_status_file=$g_zxfer_temp_file_result

	l_status=0
	zxfer_split_remote_destination_discovery_batch_stream_to_files \
		"$l_batch_status_file" \
		"$l_dest_list_tmp_file" \
		"$l_dest_list_err_file" \
		"$l_rzfs_list_hr_snap_tmp_file" \
		"$l_rzfs_list_hr_snap_err_tmp_file" <"$l_batch_output_file" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_batch_status_file"
		return "$l_status"
	fi

	l_status=0
	zxfer_load_destination_discovery_batch_status_file "$l_batch_status_file" || l_status=$?
	zxfer_cleanup_runtime_artifact_path "$l_batch_status_file"
	return "$l_status"
}

# Purpose: Record profile counters for the ZFS commands represented by one
# destination discovery batch.
# Usage: Called after parsing target-side batch status lines so `-V` output
# keeps destination `zfs list` accounting comparable with the non-batched path.
zxfer_record_remote_destination_discovery_batch_zfs_profile() {
	zxfer_profile_record_zfs_call destination list
	if [ -n "${g_zxfer_destination_discovery_batch_pool_status:-}" ]; then
		zxfer_profile_record_zfs_call destination list
	fi
	if [ "${g_zxfer_destination_discovery_batch_snapshot_ran:-0}" -eq 1 ]; then
		zxfer_profile_record_zfs_call destination list
	fi
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

	if [ "$l_destination_pool" = "" ]; then
		l_destination_pool=$g_destination
	fi

	l_status=0
	l_remote_script=$(zxfer_build_remote_destination_discovery_batch_script \
		"$g_destination" "$l_destination_dataset" "$l_destination_pool") ||
		l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_status=0
	l_remote_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_script") ||
		l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_status=0
	l_transport_tokens=$(zxfer_get_ssh_transport_tokens_for_host "$g_option_T_target_host") ||
		l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_throw_error "$l_transport_tokens" "$l_status"
	fi
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

	l_status=0
	zxfer_get_temp_file >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_transport_status_file=$g_zxfer_temp_file_result
	l_status=0
	zxfer_get_temp_file >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_transport_status_file"
		return "$l_status"
	fi
	l_transport_stderr_file=$g_zxfer_temp_file_result
	l_status=0
	zxfer_get_temp_file >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file"
		return "$l_status"
	fi
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

	l_status=0
	zxfer_read_snapshot_discovery_capture_file "$l_transport_status_file" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file" "$l_batch_status_file"
		return "$l_status"
	fi
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
		l_status=0
		zxfer_read_snapshot_discovery_capture_file "$l_transport_stderr_file" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file" "$l_batch_status_file"
			return "$l_status"
		fi
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
	zxfer_record_remote_destination_discovery_batch_zfs_profile
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
		l_status=0
		zxfer_read_snapshot_discovery_capture_file "$l_dest_list_tmp_file" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_throw_error "Failed to read staged destination dataset inventory." "$l_status"
		fi
		g_recursive_dest_list=$g_zxfer_snapshot_discovery_file_read_result
		[ -n "$g_recursive_dest_list" ] || {
			zxfer_throw_error "Staged destination dataset inventory was empty."
		}
		zxfer_seed_destination_existence_cache_from_recursive_list "$g_destination" "$g_recursive_dest_list"
		return
	fi

	l_status=0
	zxfer_read_snapshot_discovery_capture_file "$l_dest_list_err_file" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_throw_error "Failed to read staged destination dataset inventory stderr." "$l_status"
	fi
	l_dest_err=$g_zxfer_snapshot_discovery_file_read_result
	if zxfer_destination_probe_reports_missing "$l_dest_err"; then
		if [ -z "$l_dest_pool_status" ]; then
			l_dest_pool=${g_destination%%/*}
			if [ "$l_dest_pool" = "" ]; then
				l_dest_pool=$g_destination
			fi
			l_dest_pool_status=0
			zxfer_run_destination_zfs_cmd list -H -o name "$l_dest_pool" >/dev/null 2>&1 ||
				l_dest_pool_status=$?
		fi
		if [ "$l_dest_pool_status" -eq 0 ]; then
			g_recursive_dest_list=""
			zxfer_mark_destination_root_missing_in_cache "$g_destination"
			zxfer_echoV "Destination dataset missing; treating as empty list for bootstrap."
		else
			zxfer_throw_error "Failed to retrieve list of datasets from the destination" "$l_dest_pool_status"
		fi
	else
		zxfer_throw_error "Failed to retrieve list of datasets from the destination" "$l_dest_inventory_status"
	fi
}

# Purpose: Collect and publish destination dataset inventory through the local
# destination execution path.
# Usage: Called after snapshot diffing when later work has proven it can
# consume the recursive destination existence cache.
zxfer_collect_local_destination_dataset_inventory() {
	l_status=0
	zxfer_create_temp_file_group 2 >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_destination_inventory_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_dest_list_tmp_file
		IFS= read -r l_dest_list_err_file
	} <<-EOF
		$l_destination_inventory_stage_files
	EOF

	l_cmd=$(zxfer_render_destination_zfs_command list -t filesystem,volume -Hr -o name "$g_destination")
	zxfer_echoV "Running command: $l_cmd"
	zxfer_record_last_command_string "$l_cmd"
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

	l_status=0
	zxfer_create_temp_file_group 4 >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_refine_stage_files=$g_zxfer_temp_file_group_result
	{
		IFS= read -r l_common_datasets_tmp_file
		IFS= read -r l_already_changed_datasets_tmp_file
		IFS= read -r l_candidate_datasets_tmp_file
		IFS= read -r l_common_snapshot_records_tmp_file
	} <<-EOF
		$l_refine_stage_files
	EOF

	l_status=0
	LC_ALL=C comm -12 "$l_source_sorted_file" "$l_destination_sorted_file" >"$l_common_snapshot_records_tmp_file" ||
		l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
		zxfer_throw_error "Failed to derive recursive common snapshot list for snapshot identity validation." "$l_status"
	fi
	l_status=0
	zxfer_capture_recursive_dataset_list_from_snapshot_file "$l_common_snapshot_records_tmp_file" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
		zxfer_throw_error "Failed to derive recursive common dataset list for snapshot identity validation." "$l_status"
	fi
	l_status=0
	zxfer_write_recursive_dataset_list_result_to_file "$l_common_datasets_tmp_file" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
		zxfer_throw_error "Failed to stage recursive common dataset list for snapshot identity validation." "$l_status"
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
		l_status=0
		zxfer_capture_recursive_dataset_list_from_snapshot_records "$l_already_changed_datasets" ||
			l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
			zxfer_throw_error "Failed to derive already-changed recursive dataset list for snapshot identity validation." "$l_status"
		fi
		l_status=0
		zxfer_write_recursive_dataset_list_result_to_file "$l_already_changed_datasets_tmp_file" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
			zxfer_throw_error "Failed to stage already-changed recursive dataset list for snapshot identity validation." "$l_status"
		fi
	else
		l_status=0
		zxfer_write_runtime_artifact_file "$l_already_changed_datasets_tmp_file" "" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
			zxfer_throw_error "Failed to stage empty already-changed recursive dataset list for snapshot identity validation." "$l_status"
		fi
	fi

	l_status=0
	LC_ALL=C comm -23 "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" >"$l_candidate_datasets_tmp_file" ||
		l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
		zxfer_throw_error "Failed to derive recursive candidate dataset list for snapshot identity validation." "$l_status"
	fi
	if [ "$g_option_x_exclude_datasets" != "" ]; then
		l_status=0
		zxfer_capture_recursive_dataset_list_from_lines_file "$l_candidate_datasets_tmp_file" ||
			l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
			zxfer_throw_error "Failed to load recursive candidate dataset list for snapshot identity validation filtering." "$l_status"
		fi
		l_status=0
		zxfer_filter_recursive_dataset_list_with_excludes "$g_zxfer_recursive_dataset_list_result" ||
			l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
			zxfer_throw_error "Failed to filter recursive candidate dataset list against exclude patterns during snapshot identity validation." "$l_status"
		fi
		l_status=0
		zxfer_write_recursive_dataset_list_result_to_file "$l_candidate_datasets_tmp_file" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
			zxfer_throw_error "Failed to stage filtered recursive candidate dataset list for snapshot identity validation." "$l_status"
		fi
	fi

	l_status=0
	zxfer_read_snapshot_discovery_capture_file "$l_candidate_datasets_tmp_file" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
		zxfer_throw_error "Failed to read staged recursive candidate dataset list for snapshot identity validation." "$l_status"
	fi

	l_identity_validation_status=0
	while IFS= read -r l_candidate_dataset || [ -n "$l_candidate_dataset" ]; do
		[ -n "$l_candidate_dataset" ] || continue
		l_candidate_destination_dataset=$(zxfer_get_destination_dataset_for_source_dataset "$l_candidate_dataset")

		l_identity_status=0
		l_source_identity_records=$(zxfer_get_snapshot_identity_records_for_dataset source "$l_candidate_dataset") ||
			l_identity_status=$?
		if [ "$l_identity_status" -ne 0 ]; then
			l_identity_validation_status=$l_identity_status
			l_identity_validation_error="Failed to retrieve source snapshot identities for [$l_candidate_dataset]."
			break
		fi

		l_identity_status=0
		l_destination_identity_records=$(zxfer_get_snapshot_identity_records_for_dataset destination "$l_candidate_destination_dataset") ||
			l_identity_status=$?
		if [ "$l_identity_status" -ne 0 ]; then
			l_identity_validation_status=$l_identity_status
			l_identity_validation_error="Failed to retrieve destination snapshot identities for [$l_candidate_destination_dataset]."
			break
		fi

		l_identity_status=0
		zxfer_create_temp_file_group 2 >/dev/null || l_identity_status=$?
		if [ "$l_identity_status" -ne 0 ]; then
			l_identity_validation_status=$l_identity_status
			case "${g_zxfer_temp_file_group_allocated_count:-0}" in
			1)
				l_identity_validation_error="Failed to allocate destination snapshot identity staging for [$l_candidate_destination_dataset]."
				;;
			*)
				l_identity_validation_error="Failed to allocate source snapshot identity staging for [$l_candidate_dataset]."
				;;
			esac
			break
		fi
		l_identity_stage_files=$g_zxfer_temp_file_group_result
		{
			IFS= read -r l_source_identity_tmp_file
			IFS= read -r l_destination_identity_tmp_file
		} <<-EOF
			$l_identity_stage_files
		EOF
		l_identity_status=0
		zxfer_write_snapshot_identity_file_from_records "$l_source_identity_records" "$l_source_identity_tmp_file" ||
			l_identity_status=$?
		if [ "$l_identity_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_identity_stage_files"
			l_identity_validation_status=$l_identity_status
			l_identity_validation_error="Failed to write source snapshot identities for [$l_candidate_dataset]."
			break
		fi
		l_identity_status=0
		zxfer_write_snapshot_identity_file_from_records "$l_destination_identity_records" "$l_destination_identity_tmp_file" ||
			l_identity_status=$?
		if [ "$l_identity_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_identity_stage_files"
			l_identity_validation_status=$l_identity_status
			l_identity_validation_error="Failed to write destination snapshot identities for [$l_candidate_destination_dataset]."
			break
		fi

		l_identity_status=0
		l_source_identity_diff=$(zxfer_diff_snapshot_lists "$l_source_identity_tmp_file" "$l_destination_identity_tmp_file" "source_minus_destination") ||
			l_identity_status=$?
		if [ "$l_identity_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_identity_stage_files"
			l_identity_validation_status=$l_identity_status
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

		l_identity_status=0
		l_destination_identity_diff=$(zxfer_diff_snapshot_lists "$l_source_identity_tmp_file" "$l_destination_identity_tmp_file" "destination_minus_source") ||
			l_identity_status=$?
		if [ "$l_identity_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_identity_stage_files"
			l_identity_validation_status=$l_identity_status
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

		zxfer_cleanup_runtime_artifact_path_list "$l_identity_stage_files"
	done <<EOF
$g_zxfer_snapshot_discovery_file_read_result
EOF

	if [ -n "$l_identity_validation_error" ]; then
		zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
		zxfer_throw_error "$l_identity_validation_error" "$l_identity_validation_status"
	fi

	if [ -n "$l_identity_source_datasets" ]; then
		l_status=0
		zxfer_capture_recursive_dataset_list_from_snapshot_records "$(printf '%s\n%s\n' "$g_recursive_source_list" "$l_identity_source_datasets")" ||
			l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
			zxfer_throw_error "Failed to merge recursive source dataset list after snapshot identity validation." "$l_status"
		fi
		g_recursive_source_list=$g_zxfer_recursive_dataset_list_result
	fi

	if [ -n "$l_identity_destination_datasets" ]; then
		l_status=0
		zxfer_capture_recursive_dataset_list_from_snapshot_records "$(printf '%s\n%s\n' "$g_recursive_destination_extra_dataset_list" "$l_identity_destination_datasets")" ||
			l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
			zxfer_throw_error "Failed to merge recursive destination dataset list after snapshot identity validation." "$l_status"
		fi
		g_recursive_destination_extra_dataset_list=$g_zxfer_recursive_dataset_list_result
	fi

	zxfer_cleanup_runtime_artifact_path_list "$l_refine_stage_files"
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
		l_status=0
		l_cmd=$(zxfer_render_destination_zfs_command list -Hr -o name,guid -t snapshot "$l_destination_dataset") ||
			l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_throw_error "${l_cmd:-Failed to render dry-run destination snapshot discovery command.}" "$l_status"
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
	l_status=0
	l_destination_exists=$(zxfer_exists_destination "$l_destination_dataset") || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_throw_error "$l_destination_exists" "$l_status"
	fi

	if [ "$l_destination_exists" -eq 1 ]; then
		# dataset exists
		# Keep destination-side snapshot listing serial here. The older parallel
		# variant added complexity and was not a net win once metadata was cached.
		l_cmd=$(zxfer_render_destination_zfs_command list -Hr -o name,guid -t snapshot "$l_destination_dataset")
		zxfer_echoV "Running command: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
		# make sure to eval and then pipe the contents to the file in case
		# the command uses ssh
		l_status=0
		zxfer_run_destination_zfs_cmd list -Hr -o name,guid -t snapshot "$l_destination_dataset" >"$l_rzfs_list_hr_snap_tmp_file" ||
			l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_throw_error "Failed to retrieve snapshot list from the destination." "$l_status"
		fi

	else
		# dataset does not exist
		zxfer_echoV "Destination dataset does not exist: $l_destination_dataset"
		l_status=0
		zxfer_write_runtime_artifact_file "$l_rzfs_list_hr_snap_tmp_file" "" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_throw_error "Failed to stage empty destination snapshot list." "$l_status"
		fi
	fi

	l_status=0
	zxfer_normalize_destination_snapshot_list "$l_destination_dataset" "$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" ||
		l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
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
		l_status=0
		zxfer_reverse_numbered_file_lines_with_awk "$l_input_tmp_file" || l_status=$?
	else
		l_status=0
		zxfer_reverse_numbered_file_lines_with_sort "$l_input_tmp_file" || l_status=$?
	fi
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

	l_status=0
	zxfer_reverse_numbered_file_lines_with_sort "$l_numbered_tmp_file" || l_status=$?
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

	zxfer_publish_recursive_dataset_list_from_snapshot_discovery_read_result

	zxfer_cleanup_runtime_artifact_path "$l_dataset_list_sorted_file"
	return 0
}

# Purpose: Write dataset names extracted from snapshot records to a staged file.
# Usage: Called during source and destination snapshot discovery before
# recursive dataset-list helpers sort, deduplicate, and publish dataset state.
zxfer_write_recursive_dataset_lines_from_snapshot_file() {
	l_snapshot_records_file=$1
	l_dataset_lines_file=$2

	# shellcheck disable=SC2016  # awk script should see literal $1.
	"$g_cmd_awk" -F@ '{print $1}' "$l_snapshot_records_file" >"$l_dataset_lines_file"
}

# Purpose: Capture the recursive dataset list from a snapshot-record file.
# Usage: Called by in-memory and file-backed snapshot discovery paths so they
# share dataset-line staging, recursive capture, and cleanup behavior.
zxfer_capture_recursive_dataset_list_from_snapshot_record_file() {
	l_snapshot_records_file=$1

	if zxfer_get_temp_file >/dev/null; then
		:
	else
		l_status=$?
		return "$l_status"
	fi
	l_dataset_lines_file=$g_zxfer_temp_file_result

	if zxfer_write_recursive_dataset_lines_from_snapshot_file "$l_snapshot_records_file" "$l_dataset_lines_file"; then
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

	if zxfer_write_runtime_artifact_file "$l_snapshot_records_file" "$l_snapshot_records
"; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_snapshot_records_file"
		return "$l_status"
	fi

	if zxfer_capture_recursive_dataset_list_from_snapshot_record_file "$l_snapshot_records_file"; then
		:
	else
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_snapshot_records_file"
		return "$l_status"
	fi

	zxfer_cleanup_runtime_artifact_path "$l_snapshot_records_file"
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

	zxfer_capture_recursive_dataset_list_from_snapshot_record_file "$l_snapshot_records_file"
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

	l_status=0
	zxfer_create_temp_file_group 2 >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
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

	if zxfer_read_snapshot_discovery_capture_file "$l_dataset_list_filtered_file"; then
		:
	else
		l_read_status=$?
		zxfer_cleanup_runtime_artifact_path_list_and_return "$l_read_status" "$l_filter_stage_files"
		return "$?"
	fi

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
	if [ "${g_option_U_skip_unsupported_properties:-0}" -eq 1 ]; then
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

	l_status=0
	zxfer_get_temp_file >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_source_snaps_sorted_tmp_file=$g_zxfer_temp_file_result

	# sort the source snapshots for use with comm
	# wait until background processes are finished before attempting to sort
	l_cmd=$(zxfer_render_command_for_report "LC_ALL=C" sort "$l_lzfs_list_hr_s_snap_tmp_file")
	zxfer_echoV "Running command: $l_cmd > $(zxfer_quote_token_for_report "$l_source_snaps_sorted_tmp_file")"
	zxfer_record_last_command_string "$l_cmd > $(zxfer_quote_token_for_report "$l_source_snaps_sorted_tmp_file")"
	l_status=0
	LC_ALL=C sort "$l_lzfs_list_hr_s_snap_tmp_file" >"$l_source_snaps_sorted_tmp_file" ||
		l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
		zxfer_throw_error "Failed to sort source snapshots for recursive delta planning." "$l_status"
	fi

	l_status=0
	l_missing_snapshots=$(zxfer_diff_snapshot_lists "$l_source_snaps_sorted_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" "source_minus_destination") ||
		l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
		zxfer_throw_error "Failed to diff source and destination snapshots for recursive transfer planning." "$l_status"
	fi
	l_status=0
	l_destination_extra_snapshots=$(zxfer_diff_snapshot_lists "$l_source_snaps_sorted_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" "destination_minus_source") ||
		l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
		zxfer_throw_error "Failed to diff destination and source snapshots for recursive delete planning." "$l_status"
	fi
	if [ "$l_missing_snapshots" != "" ]; then
		l_status=0
		zxfer_capture_recursive_dataset_list_from_snapshot_records "$l_missing_snapshots" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
			zxfer_throw_error "Failed to derive recursive source dataset transfer list." "$l_status"
		fi
		g_recursive_source_list=$g_zxfer_recursive_dataset_list_result
	else
		g_recursive_source_list=""
	fi
	if [ "$l_destination_extra_snapshots" != "" ]; then
		l_status=0
		zxfer_capture_recursive_dataset_list_from_snapshot_records "$l_destination_extra_snapshots" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
			zxfer_throw_error "Failed to derive recursive destination dataset delete list." "$l_status"
		fi
		g_recursive_destination_extra_dataset_list=$g_zxfer_recursive_dataset_list_result
	else
		g_recursive_destination_extra_dataset_list=""
	fi
	if zxfer_snapshot_discovery_needs_source_dataset_inventory; then
		l_status=0
		zxfer_capture_recursive_dataset_list_from_snapshot_file "$l_source_snaps_sorted_tmp_file" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
			zxfer_throw_error "Failed to derive recursive source dataset inventory." "$l_status"
		fi
		g_recursive_source_dataset_list=$g_zxfer_recursive_dataset_list_result
	else
		g_recursive_source_dataset_list=""
	fi

	# if excluding datasets, remove them from the list
	if [ "$g_option_x_exclude_datasets" != "" ]; then
		l_status=0
		zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_source_list" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
			zxfer_throw_error "Failed to filter recursive source dataset transfer list against exclude patterns." "$l_status"
		fi
		g_recursive_source_list=$g_zxfer_recursive_dataset_list_result
		l_status=0
		zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_destination_extra_dataset_list" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
			zxfer_throw_error "Failed to filter recursive destination dataset delete list against exclude patterns." "$l_status"
		fi
		g_recursive_destination_extra_dataset_list=$g_zxfer_recursive_dataset_list_result
		if zxfer_snapshot_discovery_needs_source_dataset_inventory; then
			l_status=0
			zxfer_filter_recursive_dataset_list_with_excludes "$g_recursive_source_dataset_list" || l_status=$?
			if [ "$l_status" -ne 0 ]; then
				zxfer_cleanup_runtime_artifact_path "$l_source_snaps_sorted_tmp_file"
				zxfer_throw_error "Failed to filter recursive source dataset inventory against exclude patterns." "$l_status"
			fi
			g_recursive_source_dataset_list=$g_zxfer_recursive_dataset_list_result
		fi
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

# Purpose: Build the source and destination snapshot inventories that the rest
# of replication planning depends on.
# Usage: Called during source and destination snapshot discovery near the start
# of each live pass so later delete, seed, and send decisions work from one
# shared discovery result.
#
# Build the source and destination snapshot caches used by replication.
# zxfer relies on `zfs list` in machine-readable mode (`-H`), recursive dataset
# traversal (`-r`) where needed, name-only output during initial discovery
# (`-o name,guid`),
# snapshot-only listing (`-t snapshot`), and creation-order sorting for
# per-dataset snapshot discovery on the source side. Carrying GUIDs in the
# initial diff lets same-name/different-snapshot divergence become part of the
# recursive work queue without restoring a whole-tree per-dataset validation
# pass on the no-op path.
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
	l_status=0
	zxfer_create_temp_file_group 2 >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
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
	l_source_snapshot_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	l_status=0
	zxfer_write_source_snapshot_list_to_file "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" ||
		l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path_list_and_return "$l_status" "$l_source_snapshot_stage_files"
		return "$?"
	fi

	#
	# Run as many commands prior to the wait command as possible.
	#

	l_destination_snapshot_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	l_destination_inventory_attempted=0
	l_destination_dataset=$(zxfer_get_destination_snapshot_root_dataset)
	l_status=0
	zxfer_get_temp_file >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file"
		return "$l_status"
	fi
	l_rzfs_list_hr_snap_tmp_file=$g_zxfer_temp_file_result
	l_status=0
	zxfer_get_temp_file >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
			"$l_rzfs_list_hr_snap_tmp_file"
		return "$l_status"
	fi
	l_dest_snaps_stripped_sorted_tmp_file=$g_zxfer_temp_file_result

	if [ -n "${g_option_T_target_host:-}" ]; then
		l_status=0
		zxfer_create_temp_file_group 2 >/dev/null || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
				"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
			return "$l_status"
		fi
		l_destination_inventory_stage_files=$g_zxfer_temp_file_group_result
		{
			IFS= read -r l_dest_list_tmp_file
			IFS= read -r l_dest_list_err_file
		} <<-EOF
			$l_destination_inventory_stage_files
		EOF
		l_cmd=$(zxfer_render_destination_zfs_command list -t filesystem,volume -Hr -o name "$g_destination")
		zxfer_echoV "Running command: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
		l_rzfs_list_hr_snap_err_tmp_file=""
		l_status=0
		zxfer_get_temp_file >/dev/null || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path_list "$l_destination_inventory_stage_files"
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
				"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
			return "$l_status"
		fi
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
		l_status=0
		zxfer_normalize_destination_snapshot_list "$l_destination_dataset" "$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" ||
			l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
				"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" "$l_rzfs_list_hr_snap_err_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			return "$l_status"
		fi
		zxfer_cleanup_runtime_artifact_path "$l_rzfs_list_hr_snap_err_tmp_file"
	else
		# this function writes to both files passed as parameters
		l_status=0
		zxfer_write_destination_snapshot_list_to_files "$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" ||
			l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
				"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			return "$l_status"
		fi
	fi
	zxfer_profile_add_elapsed_ms g_zxfer_profile_destination_snapshot_listing_ms "$l_destination_snapshot_stage_start_ms"

	zxfer_echoV "Waiting for background processes to finish."
	l_source_snapshot_wait_status=0
	l_source_snapshot_wait_report_failure=""
	if [ -n "${g_source_snapshot_list_job_id:-}" ]; then
		l_wait_helper_status=0
		zxfer_wait_for_background_job "$g_source_snapshot_list_job_id" || l_wait_helper_status=$?
		if [ "$l_wait_helper_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
				"$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			zxfer_throw_error "Failed to read source snapshot discovery completion metadata." "$l_wait_helper_status"
		fi
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
		l_source_stderr_read_status=0
		zxfer_read_snapshot_discovery_capture_file "$l_lzfs_list_hr_s_snap_err_tmp_file" ||
			l_source_stderr_read_status=$?
		if [ "$l_source_stderr_read_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_path "$l_lzfs_list_hr_s_snap_err_tmp_file"
			zxfer_throw_error "Failed to read staged source snapshot stderr." "$l_source_stderr_read_status"
		fi
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
		zxfer_throw_error "Failed to retrieve snapshots from the source"
	fi

	#
	# END background process
	#
	l_snapshot_diff_sort_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	l_status=0
	zxfer_set_g_recursive_source_list "$l_lzfs_list_hr_s_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" ||
		l_status=$?
	zxfer_profile_add_elapsed_ms g_zxfer_profile_snapshot_diff_sort_ms "$l_snapshot_diff_sort_stage_start_ms"
	zxfer_cleanup_runtime_artifact_paths "$l_dest_snaps_stripped_sorted_tmp_file"
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
			"$l_rzfs_list_hr_snap_tmp_file"
		zxfer_cleanup_snapshot_record_cache_files
		return "$l_status"
	fi

	if zxfer_snapshot_discovery_needs_destination_dataset_inventory; then
		l_status=0
		zxfer_collect_local_destination_dataset_inventory || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
				"$l_rzfs_list_hr_snap_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			return "$l_status"
		fi
		l_destination_inventory_attempted=1
	fi

	if zxfer_snapshot_discovery_needs_record_caches; then
		g_zxfer_destination_snapshot_record_cache_file=$l_rzfs_list_hr_snap_tmp_file
		l_status=0
		zxfer_read_snapshot_discovery_capture_file "$l_rzfs_list_hr_snap_tmp_file" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
				"$l_lzfs_list_hr_s_snap_err_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			zxfer_throw_error "Failed to read staged destination snapshot list." "$l_status"
		fi
		g_rzfs_list_hr_snap=$g_zxfer_snapshot_discovery_file_read_result

		l_status=0
		zxfer_read_snapshot_discovery_capture_file "$l_lzfs_list_hr_s_snap_tmp_file" || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
				"$l_lzfs_list_hr_s_snap_err_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			zxfer_throw_error "Failed to read staged source snapshot list." "$l_status"
		fi
		g_lzfs_list_hr_snap=$g_zxfer_snapshot_discovery_file_read_result
		l_status=0
		zxfer_get_temp_file >/dev/null || l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" \
				"$l_lzfs_list_hr_s_snap_err_tmp_file"
			zxfer_cleanup_snapshot_record_cache_files
			return "$l_status"
		fi
		l_source_snapshot_record_cache_file=$g_zxfer_temp_file_result
		l_cmd=$(zxfer_render_command_for_report "" zxfer_reverse_file_lines "$l_lzfs_list_hr_s_snap_tmp_file")
		zxfer_echoV "Running command: $l_cmd > $(zxfer_quote_token_for_report "$l_source_snapshot_record_cache_file")"
		zxfer_record_last_command_string "$l_cmd > $(zxfer_quote_token_for_report "$l_source_snapshot_record_cache_file")"
		l_status=0
		zxfer_reverse_file_lines "$l_lzfs_list_hr_s_snap_tmp_file" >"$l_source_snapshot_record_cache_file" ||
			l_status=$?
		if [ "$l_status" -ne 0 ]; then
			zxfer_cleanup_runtime_artifact_paths "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file" \
				"$l_source_snapshot_record_cache_file"
			zxfer_cleanup_snapshot_record_cache_files
			zxfer_throw_error "Failed to stage source snapshot record cache." "$l_status"
		fi
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
