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

	if [ "$g_cmd_parallel" = "" ]; then
		printf '%s\n' "The -j option requires GNU parallel but it was not found in PATH on the local host."
		return 1
	fi

	# Ensure we are using GNU parallel and not a differing implementation.
	if ! "$g_cmd_parallel" --version 2>/dev/null | head -n 1 | grep -q "GNU parallel"; then
		printf '%s\n' "The -j option requires GNU parallel, but \"$g_cmd_parallel\" is not GNU parallel."
		return 1
	fi

	if [ "$g_option_O_origin_host" = "" ]; then
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
# Build the ZFS list command used to enumerate source snapshots based on the
# current CLI state. Separating this from execution allows tests to assert on
# the constructed pipeline without invoking ZFS.
#
build_source_snapshot_list_cmd() {
	if [ "$g_option_j_jobs" -le 1 ]; then
		zxfer_render_zfs_command_for_spec "$g_LZFS" list -Hr -o name,guid -s creation -t snapshot "$initial_source"
		return
	fi

	if ! ensure_parallel_available_for_source_jobs; then
		return 1
	fi

	if [ ! "$g_option_O_origin_host" = "" ]; then
		l_parallel_path=$g_origin_parallel_cmd
		l_remote_zfs_cmd=${g_origin_cmd_zfs:-$g_cmd_zfs}
		l_parallel_cmd=$(build_shell_command_from_argv "$l_parallel_path")
		l_remote_list_cmd=$(build_shell_command_from_argv \
			"$l_remote_zfs_cmd" list -Hr -o name "$initial_source")
		l_remote_runner_cmd=$(build_shell_command_from_argv \
			"$l_remote_zfs_cmd" list -H -o name,guid -s creation -d 1 -t snapshot "{}")
		l_remote_pipeline="$l_remote_list_cmd | $l_parallel_cmd -j $g_option_j_jobs --line-buffer $(build_shell_command_from_argv "$l_remote_runner_cmd")"
		if [ "$g_option_z_compress" -eq 1 ]; then
			l_remote_pipeline="$l_remote_pipeline | zstd -9"
		fi
		l_remote_shell_cmd=$(build_remote_sh_c_command "$l_remote_pipeline")
		l_cmd=$(build_ssh_shell_command_for_host "$g_option_O_origin_host" "$l_remote_shell_cmd") || return 1
		if [ "$g_option_z_compress" -eq 1 ]; then
			l_cmd="$l_cmd | zstd -d"
		fi
		printf '%s\n' "$l_cmd"
		return
	fi

	l_parallel_path=$g_cmd_parallel
	l_list_cmd=$(zxfer_render_zfs_command_for_spec "$g_LZFS" list -Hr -o name "$initial_source")
	l_runner_cmd=$(zxfer_render_zfs_command_for_spec "$g_LZFS" list -H -o name,guid -s creation -d 1 -t snapshot "{}")
	l_cmd="$l_list_cmd | $(build_shell_command_from_argv "$l_parallel_path") -j $g_option_j_jobs --line-buffer $(build_shell_command_from_argv "$l_runner_cmd")"
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
	zxfer_profile_increment_counter g_zxfer_profile_source_snapshot_list_commands
	zxfer_profile_record_bucket source_inspection

	#
	# it is important to get this in ascending order because when getting
	# in descending order, the datasets names are not ordered as we want.
	# Don't use -S creation for this command, instead, reverse the results below
	#
	if ! l_cmd=$(build_source_snapshot_list_cmd); then
		throw_error "$l_cmd"
	fi
	g_source_snapshot_list_cmd=$l_cmd
	if [ "$g_option_O_origin_host" != "" ]; then
		zxfer_profile_record_ssh_invocation "$g_option_O_origin_host" source
	fi

	if [ "$g_option_j_jobs" -gt 1 ]; then
		zxfer_profile_increment_counter g_zxfer_profile_source_snapshot_list_parallel_commands
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
		l_cmd=$(zxfer_render_destination_zfs_command list -Hr -o name,guid -t snapshot "$l_destination_dataset")
		echoV "Running command: $l_cmd"
		zxfer_record_last_command_string "$l_cmd"
		# make sure to eval and then pipe the contents to the file in case
		# the command uses ssh
		if ! run_destination_zfs_cmd list -Hr -o name,guid -t snapshot "$l_destination_dataset" >"$l_rzfs_list_hr_snap_tmp_file"; then
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

# Reverse a numbered line stream produced by `cat -n`. Strip the line number by
# tab-delimited field rather than a fixed character offset so large line counts
# do not truncate the first character of the payload.
reverse_numbered_line_stream() {
	LC_ALL=C sort -nr | cut -f2-
}

reverse_file_lines() {
	l_input_file=$1

	cat -n "$l_input_file" | reverse_numbered_line_stream
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
	if [ "$l_missing_snapshots" != "" ]; then
		# shellcheck disable=SC2016  # awk script should see literal $1.
		g_recursive_source_list=$(printf '%s\n' "$l_missing_snapshots" | "$g_cmd_awk" -F@ '{print $1}' | LC_ALL=C sort -u)
	else
		g_recursive_source_list=""
	fi
	# shellcheck disable=SC2016  # awk script should see literal $1.
	g_recursive_source_dataset_list=$("$g_cmd_awk" -F@ '{print $1}' "$l_source_snaps_sorted_tmp_file" | LC_ALL=C sort -u)

	# if excluding datasets, remove them from the list
	if [ "$g_option_x_exclude_datasets" != "" ]; then
		g_recursive_source_list=$(echo "$g_recursive_source_list" | grep -v -e "$g_option_x_exclude_datasets")
		g_recursive_source_dataset_list=$(echo "$g_recursive_source_dataset_list" | grep -v -e "$g_option_x_exclude_datasets")
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
		l_destination_extra_snapshots=$(diff_snapshot_lists "$l_source_snaps_sorted_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file" "destination_minus_source")
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
# traversal (`-r`) where needed, name-plus-guid output (`-o name,guid`),
# snapshot-only listing (`-t snapshot`), and creation-order sorting for
# per-dataset snapshot discovery on the source side.
#
get_zfs_list() {
	zxfer_set_failure_stage "snapshot discovery"
	echoV "Begin get_zfs_list()"
	g_source_snapshot_list_cmd=""

	# create temporary files used by the background processes
	l_lzfs_list_hr_s_snap_tmp_file=$(get_temp_file)
	l_lzfs_list_hr_s_snap_err_tmp_file=$(get_temp_file)

	#
	# BEGIN background process
	#
	g_source_snapshot_list_pid=""
	write_source_snapshot_list_to_file "$l_lzfs_list_hr_s_snap_tmp_file" "$l_lzfs_list_hr_s_snap_err_tmp_file"

	#
	# Run as many commands prior to the wait command as possible.
	#

	l_rzfs_list_hr_snap_tmp_file=$(get_temp_file)
	l_dest_snaps_stripped_sorted_tmp_file=$(get_temp_file)

	# this function writes to both files passed as parameters
	write_destination_snapshot_list_to_files "$l_rzfs_list_hr_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"

	# shellcheck disable=SC2034
	g_rzfs_list_hr_snap=$(cat "$l_rzfs_list_hr_snap_tmp_file")

	# get a list of all destination datasets recursively
	l_cmd=$(zxfer_render_destination_zfs_command list -t filesystem,volume -Hr -o name "$g_destination")
	echoV "Running command: $l_cmd"
	zxfer_record_last_command_string "$l_cmd"
	l_dest_list_tmp_file=$(get_temp_file)
	l_dest_list_err_file=$(get_temp_file)
	if run_destination_zfs_cmd list -t filesystem,volume -Hr -o name "$g_destination" >"$l_dest_list_tmp_file" 2>"$l_dest_list_err_file"; then
		g_recursive_dest_list=$(cat "$l_dest_list_tmp_file")
	else
		l_dest_err=$(cat "$l_dest_list_err_file")
		if printf '%s\n' "$l_dest_err" | grep -qi "dataset does not exist"; then
			l_dest_pool=${g_destination%%/*}
			if [ "$l_dest_pool" = "" ]; then
				l_dest_pool=$g_destination
			fi
			if run_destination_zfs_cmd list -H -o name "$l_dest_pool" >/dev/null 2>&1; then
				g_recursive_dest_list=""
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

	echoV "Waiting for background processes to finish."
	l_source_snapshot_wait_status=0
	if [ -n "${g_source_snapshot_list_pid:-}" ]; then
		wait "$g_source_snapshot_list_pid" || l_source_snapshot_wait_status=$?
		zxfer_unregister_cleanup_pid "$g_source_snapshot_list_pid"
		g_source_snapshot_list_pid=""
	fi
	echoV "Background processes finished."

	#
	# END background process
	#
	set_g_recursive_source_list "$l_lzfs_list_hr_s_snap_tmp_file" "$l_dest_snaps_stripped_sorted_tmp_file"

	# get the reversed order (not using tac due to solaris compatibility)
	g_lzfs_list_hr_S_snap=$(reverse_file_lines "$l_lzfs_list_hr_s_snap_tmp_file")

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

	if [ "$g_lzfs_list_hr_S_snap" = "" ]; then
		throw_error "Failed to retrieve snapshots from the source" 3
	fi

	if [ "$g_recursive_dest_list" = "" ]; then
		echoV "Destination dataset list is empty; assuming no existing datasets under \"$g_destination\""
	fi

	echoV "End get_zfs_list()"
}
