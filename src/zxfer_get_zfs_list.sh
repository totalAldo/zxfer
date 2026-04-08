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

# for ShellCheck
if false; then
	# shellcheck source=src/zxfer_globals.sh
	. ./zxfer_globals.sh
fi

#
# Ensure GNU parallel exists locally (for piping) and resolve the remote path
# when -j is used with -O. Fails closed when the binary cannot be located.
#
ensure_parallel_available_for_source_jobs() {
	if [ "$g_option_j_jobs" -le 1 ]; then
		return 0
	fi

	if [ "$g_option_O_origin_host" = "" ]; then
		if [ "$g_cmd_parallel" = "" ]; then
			printf '%s\n' "The -j option requires GNU parallel but it was not found in PATH on the local host."
			return 1
		fi

		# Ensure we are using GNU parallel and not a differing implementation.
		if ! "$g_cmd_parallel" --version 2>/dev/null | head -n 1 | grep -q "GNU parallel"; then
			printf '%s\n' "The -j option requires GNU parallel, but \"$g_cmd_parallel\" is not GNU parallel."
			return 1
		fi

		return 0
	fi

	if [ "$g_origin_parallel_cmd" != "" ]; then
		return 0
	fi

	if ! l_remote_parallel=$(resolve_remote_required_tool "$g_option_O_origin_host" parallel "GNU parallel" source); then
		case "$l_remote_parallel" in
		"Required dependency \"GNU parallel\" not found on host "*)
			printf '%s\n' "GNU parallel not found on origin host $g_option_O_origin_host but -j $g_option_j_jobs was requested. Install GNU parallel remotely or rerun without -j."
			;;
		*)
			printf '%s\n' "$l_remote_parallel"
			;;
		esac
		return 1
	fi

	g_origin_parallel_cmd=$l_remote_parallel
	return 0
}

#
# Choose how many datasets must exist before the more expensive per-dataset GNU
# parallel discovery path becomes worthwhile. Remote startup stays biased
# toward the single recursive list until the tree is large enough to repay the
# extra process fan-out.
#
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

zxfer_get_source_snapshot_discovery_dataset_list() {
	run_source_zfs_cmd list -Hr -t filesystem,volume -o name "$initial_source"
}

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

zxfer_build_source_snapshot_dataset_list_printf_cmd() {
	l_dataset_list=$1
	l_tokens=$(printf '%s\n%s' "printf" "%s\n")

	while IFS= read -r l_dataset || [ -n "$l_dataset" ]; do
		[ "$l_dataset" = "" ] && continue
		l_tokens=$(printf '%s\n%s' "$l_tokens" "$l_dataset")
	done <<EOF
$l_dataset_list
EOF

	quote_token_stream "$l_tokens"
}

#
# Source snapshot discovery only needs plain-text metadata. Keep send-stream
# compression semantics unchanged, but skip metadata compression on the remote
# adaptive `-j` discovery paths where startup overhead often dominates no-op
# wall-clock time.
#
zxfer_should_skip_remote_snapshot_discovery_compression() {
	l_jobs=${g_option_j_jobs:-1}

	[ "$g_option_z_compress" -eq 1 ] || return 1
	[ -n "${g_option_O_origin_host:-}" ] || return 1

	case "$l_jobs" in
	'' | *[!0-9]*)
		l_jobs=1
		;;
	esac

	[ "$l_jobs" -gt 1 ]
}

#
# Build the ZFS list command used to enumerate source snapshots based on the
# current CLI state. Separating this from execution allows tests to assert on
# the constructed pipeline without invoking ZFS.
#
build_source_snapshot_list_cmd() {
	g_source_snapshot_list_uses_parallel=0
	g_source_snapshot_list_uses_metadata_compression=0
	g_source_snapshot_list_skipped_metadata_compression=0
	l_local_serial_cmd=$(zxfer_render_zfs_command_for_spec "$g_LZFS" list -Hr -o name -s creation -t snapshot "$initial_source")

	if [ "$g_option_j_jobs" -le 1 ]; then
		printf '%s\n' "$l_local_serial_cmd"
		return
	fi

	# For local runs, fail fast on a missing GNU parallel dependency before the
	# adaptive dataset-count prepass can mask the real -j configuration error
	# with an unrelated source-dataset lookup failure.
	if [ "$g_option_O_origin_host" = "" ]; then
		if ! ensure_parallel_available_for_source_jobs; then
			return 1
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

		l_remote_zfs_cmd=${g_origin_cmd_zfs:-$g_cmd_zfs}
		l_remote_serial_cmd=$(build_shell_command_from_argv \
			"$l_remote_zfs_cmd" list -Hr -o name -s creation -t snapshot "$initial_source")
		if [ "$g_option_z_compress" -eq 1 ]; then
			if zxfer_should_skip_remote_snapshot_discovery_compression; then
				g_source_snapshot_list_skipped_metadata_compression=1
			else
				g_source_snapshot_list_uses_metadata_compression=1
				l_remote_compress_safe=${g_origin_cmd_compress_safe:-$g_cmd_compress_safe}
				l_remote_serial_cmd="$l_remote_serial_cmd | $l_remote_compress_safe"
			fi
		fi
		l_remote_shell_cmd=$(build_remote_sh_c_command "$l_remote_serial_cmd")
		l_cmd=$(build_ssh_shell_command_for_host "$g_option_O_origin_host" "$l_remote_shell_cmd") || return 1
		if [ "${g_source_snapshot_list_uses_metadata_compression:-0}" -eq 1 ]; then
			l_cmd="$l_cmd | $g_cmd_decompress_safe"
		fi
		printf '%s\n' "$l_cmd"
		return 0
	fi

	if ! ensure_parallel_available_for_source_jobs; then
		return 1
	fi

	g_source_snapshot_list_uses_parallel=1

	if [ ! "$g_option_O_origin_host" = "" ]; then
		l_parallel_path=$g_origin_parallel_cmd
		l_remote_zfs_cmd=${g_origin_cmd_zfs:-$g_cmd_zfs}
		l_parallel_cmd=$(build_shell_command_from_argv "$l_parallel_path")
		l_remote_runner_cmd=$(build_shell_command_from_argv \
			"$l_remote_zfs_cmd" list -H -o name -s creation -d 1 -t snapshot "{}")
		l_remote_parallel_cmd="$l_parallel_cmd -j $g_option_j_jobs --line-buffer $(build_shell_command_from_argv "$l_remote_runner_cmd")"
		if zxfer_should_inline_source_snapshot_dataset_list "$l_dataset_list" "$l_dataset_count"; then
			l_remote_dataset_input_cmd=$(zxfer_build_source_snapshot_dataset_list_printf_cmd "$l_dataset_list")
		else
			l_remote_dataset_input_cmd=$(build_shell_command_from_argv \
				"$l_remote_zfs_cmd" list -Hr -t filesystem,volume -o name "$initial_source")
		fi
		l_remote_pipeline="$l_remote_dataset_input_cmd | $l_remote_parallel_cmd"
		if [ "$g_option_z_compress" -eq 1 ]; then
			if zxfer_should_skip_remote_snapshot_discovery_compression; then
				g_source_snapshot_list_skipped_metadata_compression=1
			else
				g_source_snapshot_list_uses_metadata_compression=1
				l_remote_compress_safe=${g_origin_cmd_compress_safe:-$g_cmd_compress_safe}
				l_remote_pipeline="$l_remote_pipeline | $l_remote_compress_safe"
			fi
		fi
		l_remote_shell_cmd=$(build_remote_sh_c_command "$l_remote_pipeline")
		l_cmd=$(build_ssh_shell_command_for_host "$g_option_O_origin_host" "$l_remote_shell_cmd") || return 1
		if [ "${g_source_snapshot_list_uses_metadata_compression:-0}" -eq 1 ]; then
			l_cmd="$l_cmd | $g_cmd_decompress_safe"
		fi
		printf '%s\n' "$l_cmd"
		return
	fi

	l_parallel_path=$g_cmd_parallel
	l_runner_cmd=$(zxfer_render_zfs_command_for_spec "$g_LZFS" list -H -o name -s creation -d 1 -t snapshot "{}")
	l_parallel_cmd="$(build_shell_command_from_argv "$l_parallel_path") -j $g_option_j_jobs --line-buffer $(build_shell_command_from_argv "$l_runner_cmd")"
	if zxfer_should_inline_source_snapshot_dataset_list "$l_dataset_list" "$l_dataset_count"; then
		l_dataset_input_cmd=$(zxfer_build_source_snapshot_dataset_list_printf_cmd "$l_dataset_list")
	else
		l_dataset_input_cmd=$(zxfer_render_zfs_command_for_spec "$g_LZFS" list -Hr -t filesystem,volume -o name "$initial_source")
	fi
	l_cmd="$l_dataset_input_cmd | $l_parallel_cmd"
	printf '%s\n' "$l_cmd"
}
#
# Determine the source snapshots sorted by creation time. Since this
# can take a long time, the command is run in the background. In addition,
# to optimize the process, gnu parallel is used to retrieve snapshots from
# multiple datasets concurrently.
#
write_source_snapshot_list_to_file() {
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
	l_cmd_tmp_file=$(get_temp_file)
	if ! build_source_snapshot_list_cmd >"$l_cmd_tmp_file"; then
		l_cmd=$(cat "$l_cmd_tmp_file" 2>/dev/null || :)
		rm -f "$l_cmd_tmp_file"
		throw_error "$l_cmd"
	fi
	l_cmd=$(cat "$l_cmd_tmp_file" 2>/dev/null || :)
	rm -f "$l_cmd_tmp_file"
	g_source_snapshot_list_cmd=$l_cmd
	if [ "$g_option_O_origin_host" != "" ] &&
		[ "${g_source_snapshot_list_skipped_metadata_compression:-0}" -eq 1 ]; then
		echoV "Skipping remote source snapshot-list compression for adaptive metadata discovery."
	fi
	if [ "$g_option_O_origin_host" != "" ]; then
		zxfer_profile_record_ssh_invocation "$g_option_O_origin_host" source
	fi

	if [ "$g_option_j_jobs" -gt 1 ]; then
		if [ "${g_source_snapshot_list_uses_parallel:-0}" -eq 1 ]; then
			zxfer_profile_increment_counter g_zxfer_profile_source_snapshot_list_parallel_commands
		fi
		echoV "Running command in the background: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
		if [ -n "$l_errfile" ]; then
			eval "$l_cmd" >"$l_outfile" 2>"$l_errfile" &
		else
			eval "$l_cmd" >"$l_outfile" &
		fi
		g_source_snapshot_list_pid=$!
		zxfer_register_cleanup_pid "$g_source_snapshot_list_pid"
	else
		execute_background_cmd \
			"$l_cmd" \
			"$l_outfile" \
			"$l_errfile"
		g_source_snapshot_list_pid=${g_last_background_pid:-}
	fi
}

# Normalize the destination snapshot list so it can be directly compared to the
# source listing via comm. When the user provided a trailing slash on the
# source, the destination dataset already aligns and only needs stable sorting.
#
normalize_destination_snapshot_list() {
	l_destination_dataset=$1
	l_input_file=$2
	l_output_file=$3

	if [ "$g_initial_source_had_trailing_slash" -eq 1 ]; then
		l_cmd=$(zxfer_render_command_for_report "LC_ALL=C" sort "$l_input_file")
		echoV "Running command: $l_cmd > $(zxfer_quote_token_for_report "$l_output_file")"
		zxfer_record_last_command_string "$l_cmd > $(zxfer_quote_token_for_report "$l_output_file")"
		LC_ALL=C sort "$l_input_file" >"$l_output_file"
	else
		l_escaped_destination_dataset=$(printf '%s\n' "$l_destination_dataset" | sed 's/[].[^$\\*|]/\\&/g')
		l_cmd=$(zxfer_render_command_for_report "" sed -e "s|$l_escaped_destination_dataset|$initial_source|g" "$l_input_file")
		echoV "Running command: $l_cmd | LC_ALL=C sort > $(zxfer_quote_token_for_report "$l_output_file")"
		zxfer_record_last_command_string "$l_cmd | LC_ALL=C sort > $(zxfer_quote_token_for_report "$l_output_file")"
		sed -e "s|$l_escaped_destination_dataset|$initial_source|g" "$l_input_file" | LC_ALL=C sort >"$l_output_file"
	fi
}

zxfer_get_destination_snapshot_root_dataset() {
	l_source_dataset=${initial_source##*/}

	if [ "$g_initial_source_had_trailing_slash" -eq 1 ]; then
		printf '%s\n' "$g_destination"
	else
		printf '%s\n' "$g_destination/$l_source_dataset"
	fi
}

zxfer_get_destination_dataset_for_source_dataset() {
	l_source_dataset=$1
	l_destination_root_dataset=$(zxfer_get_destination_snapshot_root_dataset)

	if [ "$g_initial_source_had_trailing_slash" -eq 1 ]; then
		if [ "$l_source_dataset" = "$initial_source" ]; then
			printf '%s\n' "$l_destination_root_dataset"
		else
			printf '%s\n' "$l_destination_root_dataset${l_source_dataset#"$initial_source"}"
		fi
	else
		l_source_suffix=${l_source_dataset#"$initial_source"}
		if [ "$l_source_suffix" = "$l_source_dataset" ]; then
			printf '%s\n' "$l_destination_root_dataset"
		else
			printf '%s\n' "$l_destination_root_dataset$l_source_suffix"
		fi
	fi
}

zxfer_write_snapshot_identity_file_from_records() {
	l_snapshot_records=$1
	l_output_file=$2

	{
		while IFS= read -r l_snapshot_record; do
			[ -n "$l_snapshot_record" ] || continue
			l_snapshot_identity=$(extract_snapshot_identity "$l_snapshot_record")
			[ -n "$l_snapshot_identity" ] || continue
			printf '%s\n' "$l_snapshot_identity"
		done <<-EOF
			$(normalize_snapshot_record_list "$l_snapshot_records")
		EOF
	} | LC_ALL=C sort -u >"$l_output_file"
}

zxfer_refine_recursive_snapshot_deltas_with_identity_validation() {
	l_source_sorted_file=$1
	l_destination_sorted_file=$2
	l_common_datasets_tmp_file=$(get_temp_file)
	l_already_changed_datasets_tmp_file=$(get_temp_file)
	l_candidate_datasets_tmp_file=$(get_temp_file)
	l_identity_source_datasets=""
	l_identity_destination_datasets=""
	l_identity_validation_error=""

	LC_ALL=C comm -12 "$l_source_sorted_file" "$l_destination_sorted_file" |
		"$g_cmd_awk" -F@ "{print \$1}" | LC_ALL=C sort -u >"$l_common_datasets_tmp_file"

	if [ -n "${g_recursive_source_list:-}" ] || [ -n "${g_recursive_destination_extra_dataset_list:-}" ]; then
		printf '%s\n%s\n' "$g_recursive_source_list" "$g_recursive_destination_extra_dataset_list" |
			grep -v '^[[:space:]]*$' | LC_ALL=C sort -u >"$l_already_changed_datasets_tmp_file"
	else
		: >"$l_already_changed_datasets_tmp_file"
	fi

	LC_ALL=C comm -23 "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" >"$l_candidate_datasets_tmp_file"
	if [ "$g_option_x_exclude_datasets" != "" ]; then
		grep -v -e "$g_option_x_exclude_datasets" "$l_candidate_datasets_tmp_file" >"$l_candidate_datasets_tmp_file.filtered" || :
		mv -f "$l_candidate_datasets_tmp_file.filtered" "$l_candidate_datasets_tmp_file"
	fi

	while IFS= read -r l_candidate_dataset; do
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

		l_source_identity_tmp_file=$(get_temp_file)
		l_destination_identity_tmp_file=$(get_temp_file)
		zxfer_write_snapshot_identity_file_from_records "$l_source_identity_records" "$l_source_identity_tmp_file"
		zxfer_write_snapshot_identity_file_from_records "$l_destination_identity_records" "$l_destination_identity_tmp_file"

		if [ -n "$(diff_snapshot_lists "$l_source_identity_tmp_file" "$l_destination_identity_tmp_file" "source_minus_destination")" ]; then
			if [ -n "$l_identity_source_datasets" ]; then
				l_identity_source_datasets="$l_identity_source_datasets
$l_candidate_dataset"
			else
				l_identity_source_datasets=$l_candidate_dataset
			fi
		fi

		if [ -n "$(diff_snapshot_lists "$l_source_identity_tmp_file" "$l_destination_identity_tmp_file" "destination_minus_source")" ]; then
			if [ -n "$l_identity_destination_datasets" ]; then
				l_identity_destination_datasets="$l_identity_destination_datasets
$l_candidate_dataset"
			else
				l_identity_destination_datasets=$l_candidate_dataset
			fi
		fi

		rm -f "$l_source_identity_tmp_file" "$l_destination_identity_tmp_file"
	done <"$l_candidate_datasets_tmp_file"

	if [ -n "$l_identity_validation_error" ]; then
		rm -f "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" "$l_candidate_datasets_tmp_file"
		throw_error "$l_identity_validation_error"
	fi

	if [ -n "$l_identity_source_datasets" ]; then
		g_recursive_source_list=$(printf '%s\n%s\n' "$g_recursive_source_list" "$l_identity_source_datasets" |
			grep -v '^[[:space:]]*$' | LC_ALL=C sort -u)
	fi

	if [ -n "$l_identity_destination_datasets" ]; then
		g_recursive_destination_extra_dataset_list=$(printf '%s\n%s\n' "$g_recursive_destination_extra_dataset_list" "$l_identity_destination_datasets" |
			grep -v '^[[:space:]]*$' | LC_ALL=C sort -u)
	fi

	rm -f "$l_common_datasets_tmp_file" "$l_already_changed_datasets_tmp_file" "$l_candidate_datasets_tmp_file"
}

# We only need the snapshots of the intended destination dataset, not
# all the snapshots of the parent $g_destination.
# In addition, sorting by creation time has been removed in the
# destination since it is not needed.
# This significantly improves performance as the metadata
# doesn't need to be searched for the creation time of each snapshot.
# Parallelization support has been added and is useful in situations when
# the ARC is not populated such as when a removable disk is mounted.
write_destination_snapshot_list_to_files() {
	l_rzfs_list_hr_snap_tmp_file=$1
	l_dest_snaps_stripped_sorted_tmp_file=$2

	# determine the last dataset in $initial_source. This will be the last
	# dataset after a forward slash "/" or if no forward slash exists, then
	# is is the name of the dataset itself.
	l_source_dataset=$(echo "$initial_source" | awk -F'/' '{print $NF}')

	if [ "$g_initial_source_had_trailing_slash" -eq 1 ]; then
		# Trailing slash replicates directly into $g_destination (no child dataset)
		l_destination_dataset="$g_destination"
	else
		l_destination_dataset="$g_destination/$l_source_dataset"
	fi

	# check if the destination zfs dataset exists before listing snapshots
	if ! l_destination_exists=$(exists_destination "$l_destination_dataset"); then
		throw_error "$l_destination_exists"
	fi

	if [ "$l_destination_exists" -eq 1 ]; then
		# dataset exists
		# Keep destination-side snapshot listing serial here. The older parallel
		# variant added complexity and was not a net win once metadata was cached.
		l_cmd=$(zxfer_render_destination_zfs_command list -Hr -o name -t snapshot "$l_destination_dataset")
		echoV "Running command: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
		# make sure to eval and then pipe the contents to the file in case
		# the command uses ssh
		if ! run_destination_zfs_cmd list -Hr -o name -t snapshot "$l_destination_dataset" >"$l_rzfs_list_hr_snap_tmp_file"; then
			throw_error "Failed to retrieve snapshot list from the destination."
		fi

	else
		# dataset does not exist
		echoV "Destination dataset does not exist: $l_destination_dataset"
		echo "" >"$l_rzfs_list_hr_snap_tmp_file"
	fi

	normalize_destination_snapshot_list "$l_destination_dataset" "$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
}

# compare the source and destination snapshots and identify source datasets
# that are not in the destination. Set g_recursive_source_list to the
# datasets that contain snapshots that are not in the destination.
# Afterwards, g_recursive_source_list only contains the names of
# the datasets that need to be transferred.
#
# Diff sorted snapshot listings. The default mode returns snapshots only
# present in the source, while destination_minus_source highlights extra
# snapshots on the target.
#
diff_snapshot_lists() {
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
		throw_error "Unknown snapshot diff mode: $l_mode"
		;;
	esac
}

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

zxfer_reverse_numbered_file_lines_with_awk() {
	l_input_file=$1

	# shellcheck disable=SC2016  # awk program should see literal $0/NR.
	"${g_cmd_awk:-awk}" '{ l_line = $0; l_tab = index(l_line, "\t"); if (l_tab > 0) l_line = substr(l_line, l_tab + 1); l_lines[NR] = l_line } END { for (l_i = NR; l_i >= 1; l_i--) print l_lines[l_i] }' "$l_input_file"
}

zxfer_numbered_file_is_strictly_increasing() {
	l_input_file=$1

	# shellcheck disable=SC2016  # awk program should see literal $0/NR.
	"${g_cmd_awk:-awk}" 'BEGIN { l_prev = -1 } { l_tab = index($0, "\t"); if (l_tab <= 1) exit 1; l_number = substr($0, 1, l_tab - 1); gsub(/^[[:space:]]+/, "", l_number); gsub(/[[:space:]]+$/, "", l_number); if (l_number == "" || l_number ~ /[^0-9]/) exit 1; if (NR > 1 && (l_number + 0) <= l_prev) exit 1; l_prev = l_number + 0 }' "$l_input_file"
}

zxfer_reverse_numbered_file_lines_with_sort() {
	l_input_file=$1

	LC_ALL=C sort -nr "$l_input_file" | cut -f2-
}

# Reverse a numbered line stream produced by `cat -n`. Strip the line number by
# tab-delimited field rather than a fixed character offset so large line counts
# do not truncate the first character of the payload.
reverse_numbered_line_stream() {
	l_input_tmp_file=$(get_temp_file)
	cat >"$l_input_tmp_file" || {
		rm -f "$l_input_tmp_file"
		return 1
	}

	if zxfer_should_use_linear_reverse_for_file "$l_input_tmp_file" &&
		zxfer_numbered_file_is_strictly_increasing "$l_input_tmp_file"; then
		zxfer_reverse_numbered_file_lines_with_awk "$l_input_tmp_file"
	else
		zxfer_reverse_numbered_file_lines_with_sort "$l_input_tmp_file"
	fi
	l_status=$?
	rm -f "$l_input_tmp_file"
	return "$l_status"
}

zxfer_reverse_plain_file_lines_with_awk() {
	l_input_file=$1

	# shellcheck disable=SC2016  # awk program should see literal $0/NR.
	"${g_cmd_awk:-awk}" '{ l_lines[NR] = $0 } END { for (l_i = NR; l_i >= 1; l_i--) print l_lines[l_i] }' "$l_input_file"
}

zxfer_reverse_plain_file_lines_with_sort() {
	l_input_file=$1
	l_numbered_tmp_file=$(get_temp_file)

	if ! cat -n "$l_input_file" >"$l_numbered_tmp_file"; then
		rm -f "$l_numbered_tmp_file"
		return 1
	fi

	zxfer_reverse_numbered_file_lines_with_sort "$l_numbered_tmp_file"
	l_status=$?
	rm -f "$l_numbered_tmp_file"
	return "$l_status"
}

reverse_file_lines() {
	l_input_file=$1

	if zxfer_should_use_linear_reverse_for_file "$l_input_file"; then
		zxfer_reverse_plain_file_lines_with_awk "$l_input_file"
	else
		zxfer_reverse_plain_file_lines_with_sort "$l_input_file"
	fi
}

set_g_recursive_source_list() {
	l_lzfs_list_hr_s_snap_tmp_file=$1
	l_dest_snaps_stripped_sorted_tmp_file=$2

	l_source_snaps_sorted_tmp_file=$(get_temp_file)

	# sort the source snapshots for use with comm
	# wait until background processes are finished before attempting to sort
	l_cmd=$(zxfer_render_command_for_report "LC_ALL=C" sort "$l_lzfs_list_hr_s_snap_tmp_file")
	echoV "Running command: $l_cmd > $(zxfer_quote_token_for_report "$l_source_snaps_sorted_tmp_file")"
	zxfer_record_last_command_string "$l_cmd > $(zxfer_quote_token_for_report "$l_source_snaps_sorted_tmp_file")"
	LC_ALL=C sort "$l_lzfs_list_hr_s_snap_tmp_file" >"$l_source_snaps_sorted_tmp_file"

	l_missing_snapshots=$(diff_snapshot_lists "$l_source_snaps_sorted_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" "source_minus_destination")
	l_destination_extra_snapshots=$(diff_snapshot_lists "$l_source_snaps_sorted_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" "destination_minus_source")
	if [ "$l_missing_snapshots" != "" ]; then
		# shellcheck disable=SC2016  # awk script should see literal $1.
		g_recursive_source_list=$(printf '%s\n' "$l_missing_snapshots" | "$g_cmd_awk" -F@ '{print $1}' | LC_ALL=C sort -u)
	else
		g_recursive_source_list=""
	fi
	if [ "$l_destination_extra_snapshots" != "" ]; then
		# shellcheck disable=SC2016  # awk script should see literal $1.
		g_recursive_destination_extra_dataset_list=$(printf '%s\n' "$l_destination_extra_snapshots" | "$g_cmd_awk" -F@ '{print $1}' | LC_ALL=C sort -u)
	else
		g_recursive_destination_extra_dataset_list=""
	fi
	# shellcheck disable=SC2016  # awk script should see literal $1.
	g_recursive_source_dataset_list=$("$g_cmd_awk" -F@ '{print $1}' "$l_source_snaps_sorted_tmp_file" | LC_ALL=C sort -u)

	# if excluding datasets, remove them from the list
	if [ "$g_option_x_exclude_datasets" != "" ]; then
		g_recursive_source_list=$(echo "$g_recursive_source_list" | grep -v -e "$g_option_x_exclude_datasets")
		g_recursive_destination_extra_dataset_list=$(echo "$g_recursive_destination_extra_dataset_list" | grep -v -e "$g_option_x_exclude_datasets")
		g_recursive_source_dataset_list=$(echo "$g_recursive_source_dataset_list" | grep -v -e "$g_option_x_exclude_datasets")
	fi

	zxfer_refine_recursive_snapshot_deltas_with_identity_validation "$l_source_snaps_sorted_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"

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
		if [ "$l_destination_extra_snapshots" != "" ]; then
			# shellcheck disable=SC2016
			printf '%s\n' "$l_destination_extra_snapshots" | "$g_cmd_awk" -F@ '{print $1}' | LC_ALL=C sort -u
		fi
		echo "====================================================================="
	fi

	if [ "$g_recursive_source_list" = "" ]; then
		echov "No new snapshots to transfer."
	fi

	rm "$l_source_snaps_sorted_tmp_file" &
}

#
# Build the source and destination snapshot caches used by replication.
# zxfer relies on `zfs list` in machine-readable mode (`-H`), recursive dataset
# traversal (`-r`) where needed, name-only output during initial discovery
# (`-o name`),
# snapshot-only listing (`-t snapshot`), and creation-order sorting for
# per-dataset snapshot discovery on the source side. Guid-bearing identity
# records are fetched lazily later only for datasets that still need exact
# common-snapshot or delete validation.
#
get_zfs_list() {
	zxfer_set_failure_stage "snapshot discovery"
	echoV "Begin get_zfs_list()"
	g_source_snapshot_list_cmd=""
	g_source_snapshot_list_uses_parallel=0
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""
	zxfer_reset_destination_existence_cache
	zxfer_reset_snapshot_record_indexes

	# create temporary files used by the background processes
	l_lzfs_list_hr_s_snap_tmp_file=$(get_temp_file)
	l_lzfs_list_hr_s_snap_err_tmp_file=$(get_temp_file)

	#
	# BEGIN background process
	#
	g_source_snapshot_list_pid=""
	l_source_snapshot_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	write_source_snapshot_list_to_file "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file"

	#
	# Run as many commands prior to the wait command as possible.
	#

	# get a list of all destination datasets recursively
	l_destination_snapshot_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	l_cmd=$(zxfer_render_destination_zfs_command list -t filesystem,volume -Hr -o name "$g_destination")
	echoV "Running command: $l_cmd"
	zxfer_record_last_command_string "$l_cmd"
	l_dest_list_tmp_file=$(get_temp_file)
	l_dest_list_err_file=$(get_temp_file)
	if run_destination_zfs_cmd list -t filesystem,volume -Hr -o name "$g_destination" >"$l_dest_list_tmp_file" 2>"$l_dest_list_err_file"; then
		g_recursive_dest_list=$(cat "$l_dest_list_tmp_file")
		zxfer_seed_destination_existence_cache_from_recursive_list "$g_destination" "$g_recursive_dest_list"
	else
		l_dest_err=$(cat "$l_dest_list_err_file")
		if printf '%s\n' "$l_dest_err" | grep -qi "dataset does not exist"; then
			l_dest_pool=${g_destination%%/*}
			if [ "$l_dest_pool" = "" ]; then
				l_dest_pool=$g_destination
			fi
			if run_destination_zfs_cmd list -H -o name "$l_dest_pool" >/dev/null 2>&1; then
				g_recursive_dest_list=""
				zxfer_mark_destination_root_missing_in_cache "$g_destination"
				echoV "Destination dataset missing; treating as empty list for bootstrap."
			else
				rm -f "$l_dest_list_tmp_file" "$l_dest_list_err_file"
				throw_usage_error "Failed to retrieve list of datasets from the destination"
			fi
		else
			rm -f "$l_dest_list_tmp_file" "$l_dest_list_err_file"
			throw_usage_error "Failed to retrieve list of datasets from the destination"
		fi
	fi
	rm -f "$l_dest_list_tmp_file" "$l_dest_list_err_file"

	l_rzfs_list_hr_snap_tmp_file=$(get_temp_file)
	l_dest_snaps_stripped_sorted_tmp_file=$(get_temp_file)

	# this function writes to both files passed as parameters
	write_destination_snapshot_list_to_files "$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
	zxfer_profile_add_elapsed_ms g_zxfer_profile_destination_snapshot_listing_ms "$l_destination_snapshot_stage_start_ms"

	# shellcheck disable=SC2034
	g_rzfs_list_hr_snap=$(cat "$l_rzfs_list_hr_snap_tmp_file")

	echoV "Waiting for background processes to finish."
	l_source_snapshot_wait_status=0
	if [ -n "${g_source_snapshot_list_pid:-}" ]; then
		wait "$g_source_snapshot_list_pid" || l_source_snapshot_wait_status=$?
		zxfer_unregister_cleanup_pid "$g_source_snapshot_list_pid"
		g_source_snapshot_list_pid=""
	fi
	zxfer_profile_add_elapsed_ms g_zxfer_profile_source_snapshot_listing_ms "$l_source_snapshot_stage_start_ms"
	g_lzfs_list_hr_snap=$(cat "$l_lzfs_list_hr_s_snap_tmp_file")
	echoV "Background processes finished."

	#
	# END background process
	#
	l_snapshot_diff_sort_stage_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)
	set_g_recursive_source_list "$l_lzfs_list_hr_s_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"
	zxfer_profile_add_elapsed_ms g_zxfer_profile_snapshot_diff_sort_ms "$l_snapshot_diff_sort_stage_start_ms"

	# remove temporary files
	rm "$l_lzfs_list_hr_s_snap_tmp_file" \
		"$l_rzfs_list_hr_snap_tmp_file" \
		"$l_dest_snaps_stripped_sorted_tmp_file"

	if [ "$l_source_snapshot_wait_status" -ne 0 ]; then
		l_source_snapshot_err=$(sed -n '1,10p' "$l_lzfs_list_hr_s_snap_err_tmp_file")
		if [ -n "${g_source_snapshot_list_cmd:-}" ]; then
			zxfer_record_last_command_string "$g_source_snapshot_list_cmd"
		fi
		rm -f "$l_lzfs_list_hr_s_snap_err_tmp_file"
		if [ "$l_source_snapshot_err" != "" ]; then
			throw_error "Failed to retrieve snapshots from the source: $l_source_snapshot_err" 3
		fi
		throw_error "Failed to retrieve snapshots from the source" 3
	fi
	rm -f "$l_lzfs_list_hr_s_snap_err_tmp_file"

	#
	# Errors
	#

	if [ "$g_lzfs_list_hr_snap" = "" ]; then
		throw_error "Failed to retrieve snapshots from the source" 3
	fi

	if [ "$g_recursive_dest_list" = "" ]; then
		echoV "Destination dataset list is empty; assuming no existing datasets under \"$g_destination\""
	fi

	echoV "End get_zfs_list()"
}
