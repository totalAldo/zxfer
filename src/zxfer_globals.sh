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

# shellcheck disable=SC2034
# Global variables defined here are used across multiple zxfer modules.

################################################################################
# DEFINE GLOBALS used by zxfer
################################################################################

#
# Define global variables
#

zxfer_refresh_backup_storage_root() {
	if [ -n "${ZXFER_BACKUP_DIR:-}" ]; then
		g_backup_storage_root=$ZXFER_BACKUP_DIR
	elif [ -z "${g_backup_storage_root:-}" ]; then
		g_backup_storage_root=/var/db/zxfer
	fi
}
zxfer_reset_destination_existence_cache() {
	g_destination_existence_cache=""
	g_destination_existence_cache_root=""
	g_destination_existence_cache_root_complete=0
}

zxfer_reset_snapshot_record_indexes() {
	if [ -n "${g_zxfer_snapshot_index_dir:-}" ] && [ -d "$g_zxfer_snapshot_index_dir" ]; then
		rm -rf "$g_zxfer_snapshot_index_dir"
	fi

	g_zxfer_snapshot_index_dir=""
	g_zxfer_snapshot_index_unavailable=0
	g_zxfer_source_snapshot_record_index=""
	g_zxfer_source_snapshot_record_index_ready=0
	g_zxfer_destination_snapshot_record_index=""
	g_zxfer_destination_snapshot_record_index_ready=0
}

zxfer_ensure_snapshot_index_dir() {
	if [ "${g_zxfer_snapshot_index_unavailable:-0}" -eq 1 ]; then
		return 1
	fi

	if [ -n "${g_zxfer_snapshot_index_dir:-}" ] && [ -d "$g_zxfer_snapshot_index_dir" ]; then
		return 0
	fi

	if ! l_tmpdir=$(zxfer_try_get_effective_tmpdir); then
		g_zxfer_snapshot_index_unavailable=1
		g_zxfer_snapshot_index_dir=""
		return 1
	fi
	if ! g_zxfer_snapshot_index_dir=$(mktemp -d "$l_tmpdir/zxfer-snapshot-index.XXXXXX" 2>/dev/null); then
		g_zxfer_snapshot_index_unavailable=1
		g_zxfer_snapshot_index_dir=""
		return 1
	fi

	return 0
}

zxfer_build_snapshot_record_index() {
	l_side=$1
	l_snapshot_records=$2

	case "$l_side" in
	source)
		g_zxfer_source_snapshot_record_index=""
		g_zxfer_source_snapshot_record_index_ready=0
		;;
	destination)
		g_zxfer_destination_snapshot_record_index=""
		g_zxfer_destination_snapshot_record_index_ready=0
		;;
	*)
		return 1
		;;
	esac

	if ! zxfer_ensure_snapshot_index_dir; then
		return 1
	fi

	l_side_dir=$g_zxfer_snapshot_index_dir/$l_side
	rm -rf "$l_side_dir"
	if ! mkdir -p "$l_side_dir"; then
		g_zxfer_snapshot_index_unavailable=1
		return 1
	fi

	# shellcheck disable=SC2016  # awk program should see literal $0.
	l_index_map=$(printf '%s\n' "$l_snapshot_records" | "${g_cmd_awk:-awk}" -v index_dir="$l_side_dir" '
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
		file_paths[dataset] = index_dir "/" file_count ".records"
		dataset_order[file_count] = dataset
	}
	print record >> file_paths[dataset]
	# Avoid exhausting awk output descriptors on deep recursive trees.
	close(file_paths[dataset])
}
END {
	for (i = 1; i <= file_count; i++)
		print dataset_order[i] "\t" file_paths[dataset_order[i]]
}') || {
		g_zxfer_snapshot_index_unavailable=1
		return 1
	}

	case "$l_side" in
	source)
		g_zxfer_source_snapshot_record_index=$l_index_map
		g_zxfer_source_snapshot_record_index_ready=1
		;;
	destination)
		g_zxfer_destination_snapshot_record_index=$l_index_map
		g_zxfer_destination_snapshot_record_index_ready=1
		;;
	esac

	return 0
}

zxfer_ensure_source_snapshot_record_cache() {
	[ -n "${g_lzfs_list_hr_S_snap:-}" ] && return 0
	[ -n "${g_lzfs_list_hr_snap:-}" ] || return 1

	if ! g_lzfs_list_hr_S_snap=$(zxfer_reverse_snapshot_record_list "$g_lzfs_list_hr_snap"); then
		return 1
	fi

	[ -n "${g_lzfs_list_hr_S_snap:-}" ]
}

zxfer_ensure_snapshot_record_index_for_side() {
	l_side=$1

	case "$l_side" in
	source)
		[ "${g_zxfer_source_snapshot_record_index_ready:-0}" -eq 1 ] && return 0
		zxfer_ensure_source_snapshot_record_cache || return 1
		l_snapshot_records=$g_lzfs_list_hr_S_snap
		;;
	destination)
		[ "${g_zxfer_destination_snapshot_record_index_ready:-0}" -eq 1 ] && return 0
		l_snapshot_records=${g_rzfs_list_hr_snap:-}
		;;
	*)
		return 1
		;;
	esac

	[ -n "$l_snapshot_records" ] || return 1

	zxfer_build_snapshot_record_index "$l_side" "$l_snapshot_records"
}

zxfer_get_indexed_snapshot_records_for_dataset() {
	l_side=$1
	l_dataset=$2

	case "$l_side" in
	source)
		l_index_map=${g_zxfer_source_snapshot_record_index:-}
		l_index_ready=${g_zxfer_source_snapshot_record_index_ready:-0}
		;;
	destination)
		l_index_map=${g_zxfer_destination_snapshot_record_index:-}
		l_index_ready=${g_zxfer_destination_snapshot_record_index_ready:-0}
		;;
	*)
		return 1
		;;
	esac

	[ "$l_index_ready" -eq 1 ] || return 1

	while IFS='	' read -r l_indexed_dataset l_record_file || [ -n "${l_indexed_dataset}${l_record_file}" ]; do
		[ -n "$l_indexed_dataset" ] || continue
		if [ "$l_indexed_dataset" = "$l_dataset" ]; then
			if [ -f "$l_record_file" ]; then
				cat "$l_record_file"
				return 0
			fi
			return 1
		fi
	done <<-EOF
		$l_index_map
	EOF

	return 0
}

zxfer_get_snapshot_records_for_dataset() {
	l_side=$1
	l_dataset=$2

	if zxfer_get_indexed_snapshot_records_for_dataset "$l_side" "$l_dataset"; then
		return 0
	fi
	if zxfer_ensure_snapshot_record_index_for_side "$l_side"; then
		if zxfer_get_indexed_snapshot_records_for_dataset "$l_side" "$l_dataset"; then
			return 0
		fi
	fi

	case "$l_side" in
	source)
		zxfer_ensure_source_snapshot_record_cache || return 0
		# shellcheck disable=SC2016  # awk program should see literal $1/$0.
		printf '%s\n' "$g_lzfs_list_hr_S_snap" | "${g_cmd_awk:-awk}" -F@ -v ds="$l_dataset" '$1 == ds { print $0 }'
		;;
	destination)
		# shellcheck disable=SC2016  # awk program should see literal $1/$0.
		printf '%s\n' "$g_rzfs_list_hr_snap" | "${g_cmd_awk:-awk}" -F@ -v ds="$l_dataset" '$1 == ds { print $0 }'
		;;
	*)
		return 1
		;;
	esac
}

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

zxfer_mark_destination_root_missing_in_cache() {
	l_root_dataset=$1

	zxfer_reset_destination_existence_cache
	g_destination_existence_cache_root=$l_root_dataset
	g_destination_existence_cache_root_complete=1
	[ -n "$l_root_dataset" ] && zxfer_set_destination_existence_cache_entry "$l_root_dataset" 0
}

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

# Secure location for property backup files (override via ZXFER_BACKUP_DIR).
g_backup_storage_root=""
zxfer_refresh_backup_storage_root

# Short-lived remote capability cache used to avoid repeating helper discovery
# across closely spaced zxfer processes targeting the same host.
g_zxfer_remote_capability_cache_ttl=15
g_zxfer_remote_capability_cache_wait_retries=5
g_origin_remote_capabilities_host=""
g_origin_remote_capabilities_response=""
g_origin_remote_capabilities_bootstrap_source=""
g_target_remote_capabilities_host=""
g_target_remote_capabilities_response=""
g_target_remote_capabilities_bootstrap_source=""

# Directories considered safe for PATH lookups. Administrators may override the
# entire list via ZXFER_SECURE_PATH or append additional trusted directories via
# ZXFER_SECURE_PATH_APPEND.
ZXFER_DEFAULT_SECURE_PATH="/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

zxfer_compute_secure_path() {
	l_candidate=$ZXFER_DEFAULT_SECURE_PATH
	if [ -n "${ZXFER_SECURE_PATH:-}" ]; then
		l_candidate=$ZXFER_SECURE_PATH
	fi
	if [ -n "${ZXFER_SECURE_PATH_APPEND:-}" ]; then
		if [ "$l_candidate" = "" ]; then
			l_candidate=$ZXFER_SECURE_PATH_APPEND
		else
			l_candidate=$l_candidate:$ZXFER_SECURE_PATH_APPEND
		fi
	fi

	OLDIFS=$IFS
	IFS=":"
	l_clean=""
	for l_entry in $l_candidate; do
		case "$l_entry" in
		'' | .)
			continue
			;;
		/*)
			if [ "$l_clean" = "" ]; then
				l_clean=$l_entry
			else
				l_clean=$l_clean:$l_entry
			fi
			;;
		*)
			# Ignore relative path segments to keep PATH confined to absolute directories.
			continue
			;;
		esac
	done
	IFS=$OLDIFS

	if [ "$l_clean" = "" ]; then
		l_clean=$ZXFER_DEFAULT_SECURE_PATH
	fi

	printf '%s\n' "$l_clean"
}

merge_path_allowlists() {
	l_primary=$1
	l_secondary=$2

	OLDIFS=$IFS
	IFS=":"
	l_merged=""
	for l_entry in $l_primary $l_secondary; do
		[ -n "$l_entry" ] || continue
		case ":$l_merged:" in
		*:"$l_entry":*)
			continue
			;;
		esac
		if [ "$l_merged" = "" ]; then
			l_merged=$l_entry
		else
			l_merged=$l_merged:$l_entry
		fi
	done
	IFS=$OLDIFS

	printf '%s\n' "$l_merged"
}

zxfer_apply_secure_path() {
	g_zxfer_secure_path=$(zxfer_compute_secure_path)
	g_zxfer_dependency_path=$g_zxfer_secure_path
	g_zxfer_runtime_path=$(merge_path_allowlists "$g_zxfer_secure_path" "$ZXFER_DEFAULT_SECURE_PATH")
	PATH=$g_zxfer_runtime_path
	export PATH
}

ssh_supports_control_sockets() {
	[ -n "${g_cmd_ssh:-}" ] || return 1
	"$g_cmd_ssh" -M -V >/dev/null 2>&1
}

zxfer_validate_resolved_tool_path() {
	l_path=$1
	l_label=$2
	l_scope=${3:-}

	l_tab=$(printf '\t')
	l_cr=$(printf '\r')
	l_lf=$(printf '\n_')
	l_lf=${l_lf%_}

	case "$l_path" in
	*"$l_tab"* | *"$l_cr"* | *"$l_lf"*)
		if [ "$l_scope" = "" ]; then
			printf '%s\n' "Required dependency \"$l_label\" resolved to \"$l_path\", but zxfer requires a single-line absolute path without control whitespace."
		else
			printf '%s\n' "Required dependency \"$l_label\" on $l_scope resolved to \"$l_path\", but zxfer requires a single-line absolute path without control whitespace."
		fi
		return 1
		;;
	esac

	case "$l_path" in
	/*)
		printf '%s\n' "$l_path"
		return 0
		;;
	*)
		if [ "$l_scope" = "" ]; then
			printf '%s\n' "Required dependency \"$l_label\" resolved to \"$l_path\", but zxfer requires an absolute path."
		else
			printf '%s\n' "Required dependency \"$l_label\" on $l_scope resolved to \"$l_path\", but zxfer requires an absolute path."
		fi
		return 1
		;;
	esac
}

zxfer_find_required_tool() {
	l_tool=$1
	l_label=${2:-$l_tool}
	l_search_path=${g_zxfer_dependency_path:-$g_zxfer_secure_path}
	[ -n "$l_search_path" ] || l_search_path=$ZXFER_DEFAULT_SECURE_PATH
	l_path=$(PATH=$l_search_path command -v "$l_tool" 2>/dev/null || :)
	if [ "$l_path" = "" ]; then
		printf '%s\n' "Required dependency \"$l_label\" not found in secure PATH ($g_zxfer_secure_path). Set ZXFER_SECURE_PATH or install the binary."
		return 1
	fi

	zxfer_validate_resolved_tool_path "$l_path" "$l_label"
}

zxfer_assign_required_tool() {
	l_var_name=$1
	l_tool=$2
	l_label=${3:-$l_tool}

	if ! l_resolved_path=$(zxfer_find_required_tool "$l_tool" "$l_label"); then
		g_zxfer_failure_class=dependency
		throw_error "$l_resolved_path"
	fi

	eval "$l_var_name=\$l_resolved_path"
}

zxfer_apply_secure_path

# Some unit tests source zxfer helpers without calling init_globals(), so make
# sure the awk command resolves to something usable even before the real
# initialization logic runs.
if [ -z "${g_cmd_awk:-}" ]; then
	l_search_path=${g_zxfer_dependency_path:-$g_zxfer_secure_path}
	[ -n "$l_search_path" ] || l_search_path=$ZXFER_DEFAULT_SECURE_PATH
	g_cmd_awk=$(PATH=$l_search_path command -v awk 2>/dev/null || :)
	if [ -z "$g_cmd_awk" ]; then
		g_cmd_awk='awk'
	fi
fi

init_globals() {
	zxfer_reset_failure_context "startup"

	# zxfer version
	g_zxfer_version="2.0-20260407"

	# max number of iterations to run iterate through run_zfs_mode
	# if changes are made to the filesystems
	g_MAX_YIELD_ITERATIONS=8

	# Default values
	g_option_b_beep_always=0
	g_option_B_beep_on_success=0
	g_option_c_services=""
	g_option_d_delete_destination_snapshots=0
	g_option_D_display_progress_bar=""
	g_option_e_restore_property_mode=0
	g_option_F_force_rollback=""
	g_option_g_grandfather_protection=""
	g_option_I_ignore_properties=""
	# number of parallel job processes to run when listing zfs snapshots
	# in the source (default 1 does not use parallel).
	# This also sets the maximum number of background zfs send processes
	# that can run at the same time.
	g_option_j_jobs=1
	g_option_k_backup_property_mode=0
	g_option_o_override_property=""
	g_option_O_origin_host=""
	g_option_O_origin_host_safe=""
	g_option_P_transfer_property=0
	g_option_R_recursive=""
	g_option_m_migrate=0
	g_option_n_dryrun=0
	g_option_N_nonrecursive=""
	g_option_s_make_snapshot=0
	g_option_T_target_host=""
	g_option_T_target_host_safe=""
	g_option_U_skip_unsupported_properties=0
	g_option_v_verbose=0
	g_option_V_very_verbose=0
	g_option_x_exclude_datasets=""
	g_option_Y_yield_iterations=1
	g_option_w_raw_send=0
	g_option_z_compress=0

	# services stopped by -c/-m that must be restarted on exit
	g_services_need_relaunch=0
	g_services_relaunch_in_progress=0

	source=""
	initial_source=""
	g_initial_source_had_trailing_slash=0
	g_cmd_zfs=""

	# keep track of the number of background zfs send jobs
	g_count_zfs_send_jobs=0
	g_zfs_send_job_pids=""
	g_zfs_send_job_records=""
	g_zfs_send_job_queue_open=0
	g_zfs_send_job_queue_unavailable=0
	g_destination_existence_cache=""
	g_destination_existence_cache_root=""
	g_destination_existence_cache_root_complete=0
	g_destination_snapshot_creation_cache=""
	g_zxfer_snapshot_index_dir=""
	g_zxfer_snapshot_index_unavailable=0
	g_zxfer_source_snapshot_record_index=""
	g_zxfer_source_snapshot_record_index_ready=0
	g_zxfer_destination_snapshot_record_index=""
	g_zxfer_destination_snapshot_record_index_ready=0
	g_origin_remote_capabilities_host=""
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_bootstrap_source=""
	g_source_snapshot_list_uses_parallel=0
	g_zxfer_cleanup_pids=""
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	g_zxfer_profile_start_epoch=$(date '+%s' 2>/dev/null || :)
	g_zxfer_profile_has_data=0
	g_zxfer_profile_summary_emitted=0
	g_zxfer_profile_ssh_setup_ms=0
	g_zxfer_profile_source_snapshot_listing_ms=0
	g_zxfer_profile_destination_snapshot_listing_ms=0
	g_zxfer_profile_snapshot_diff_sort_ms=0
	g_zxfer_profile_source_zfs_calls=0
	g_zxfer_profile_destination_zfs_calls=0
	g_zxfer_profile_other_zfs_calls=0
	g_zxfer_profile_zfs_list_calls=0
	g_zxfer_profile_zfs_get_calls=0
	g_zxfer_profile_zfs_send_calls=0
	g_zxfer_profile_zfs_receive_calls=0
	g_zxfer_profile_ssh_shell_invocations=0
	g_zxfer_profile_source_ssh_shell_invocations=0
	g_zxfer_profile_destination_ssh_shell_invocations=0
	g_zxfer_profile_other_ssh_shell_invocations=0
	g_zxfer_profile_source_snapshot_list_commands=0
	g_zxfer_profile_source_snapshot_list_parallel_commands=0
	g_zxfer_profile_send_receive_pipeline_commands=0
	g_zxfer_profile_send_receive_background_pipeline_commands=0
	g_zxfer_profile_exists_destination_calls=0
	g_zxfer_profile_normalized_property_reads_source=0
	g_zxfer_profile_normalized_property_reads_destination=0
	g_zxfer_profile_normalized_property_reads_other=0
	g_zxfer_profile_required_property_backfill_gets=0
	g_zxfer_profile_parent_destination_property_reads=0
	g_zxfer_profile_bucket_source_inspection=0
	g_zxfer_profile_bucket_destination_inspection=0
	g_zxfer_profile_bucket_property_reconciliation=0
	g_zxfer_profile_bucket_send_receive_setup=0
	g_zxfer_property_cache_dir=""
	g_zxfer_property_cache_unavailable=0
	g_zxfer_source_property_tree_prefetch_root=""
	g_zxfer_source_property_tree_prefetch_zfs_cmd=""
	g_zxfer_source_property_tree_prefetch_state=0
	g_zxfer_destination_property_tree_prefetch_root=""
	g_zxfer_destination_property_tree_prefetch_zfs_cmd=""
	g_zxfer_destination_property_tree_prefetch_state=0

	g_destination=""
	g_backup_file_extension=".zxfer_backup_info"
	g_backup_file_contents=""
	zxfer_refresh_backup_storage_root
	g_dest_seed_requires_property_reconcile=0

	# operating systems
	g_source_operating_system=""
	g_destination_operating_system=""

	# default compression commands
	g_cmd_compress="zstd -3"
	g_cmd_decompress="zstd -d"
	g_cmd_compress_safe=""
	g_cmd_decompress_safe=""
	g_origin_cmd_compress_safe=""
	g_origin_cmd_decompress_safe=""
	g_target_cmd_compress_safe=""
	g_target_cmd_decompress_safe=""

	g_cmd_cat=""

	zxfer_assign_required_tool g_cmd_awk awk "awk"
	zxfer_assign_required_tool g_cmd_zfs zfs "zfs"
	g_cmd_parallel=$(PATH=$g_zxfer_dependency_path command -v parallel 2>/dev/null || :)
	if [ "$g_cmd_parallel" != "" ]; then
		if ! g_cmd_parallel=$(zxfer_validate_resolved_tool_path "$g_cmd_parallel" "GNU parallel"); then
			g_zxfer_failure_class=dependency
			throw_error "$g_cmd_parallel"
		fi
	fi
	g_origin_parallel_cmd=""
	# enable compression in ssh options so that remote snapshot lists that
	# contain thousands of snapshots are compressed
	zxfer_assign_required_tool g_cmd_ssh ssh "ssh"
	# ssh control sockets used for origin (-O) and target (-T) hosts
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""
	g_ssh_origin_control_socket_lease_file=""
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_dir=""
	g_ssh_target_control_socket_lease_file=""
	g_ssh_supports_control_sockets=0
	if ssh_supports_control_sockets; then
		g_ssh_supports_control_sockets=1
	fi

	# default zfs commands, can be overridden by -O or -T
	g_LZFS=$g_cmd_zfs
	g_RZFS=$g_cmd_zfs
	g_origin_cmd_zfs=$g_cmd_zfs
	g_target_cmd_zfs=$g_cmd_zfs

	# dataset and snapshot lists
	g_recursive_source_list=""
	g_recursive_source_dataset_list=""
	g_recursive_destination_extra_dataset_list=""
	g_lzfs_list_hr_snap=""
	g_lzfs_list_hr_S_snap=""
	g_rzfs_list_hr_snap=""

	g_last_common_snap=""
	g_dest_has_snapshots=0
	g_did_delete_dest_snapshots=0
	g_deleted_dest_newer_snapshots=0
	g_actual_dest=""
	g_src_snapshot_transfer_list=""
	g_pending_receive_create_opts=""
	g_pending_receive_create_dest=""
	g_zxfer_temp_prefix="zxfer.$$.${g_option_Y_yield_iterations}.$(date +%s)"

	# temporary files used by get_dest_snapshots_to_delete_per_dataset()
	g_delete_source_tmp_file=$(get_temp_file)
	g_delete_dest_tmp_file=$(get_temp_file)
	g_delete_snapshots_to_delete_tmp_file=$(get_temp_file)

	# specific to zfs mode
	g_zxfer_new_snapshot_name=zxfer_$$_$(date +%Y%m%d%H%M%S)

	g_restored_backup_file_contents=""

	g_ensure_writable=0 # when creating/setting properties, ensures readonly=off

	# the readonly properties list 3 properties that are technically not
	# readonly but we will remove them from the override list as it does not make
	# sense to try and transfer them - version, volsize and mountpoint
	# Others have been added since. This is a potential refactor point
	# to split into two lists, $g_readonly_properties and $zxfer_unsupported_properties
	g_readonly_properties="type,creation,used,available,referenced,\
compressratio,mounted,version,primarycache,secondarycache,\
usedbysnapshots,usedbydataset,usedbychildren,usedbyrefreservation,\
version,volsize,mountpoint,mlslabel,keysource,keystatus,rekeydate,encryption,encryptionroot,keylocation,keyformat,pbkdf2iters,snapshots_changed,special_small_blocks,\
refcompressratio,written,logicalused,logicalreferenced,createtxg,guid,origin,\
filesystem_count,snapshot_count,clones,defer_destroy,receive_resume_token,\
userrefs,objsetid"

	# Properties not supported on FreeBSD
	g_fbsd_readonly_properties="aclmode,aclinherit,devices,nbmand,shareiscsi,vscan,\
xattr,dnodesize"

	# Properties not supported on Solaris Express 11
	g_solexp_readonly_properties="jailed,aclmode,shareiscsi"

	refresh_compression_commands
}

refresh_compression_commands() {
	if [ "$g_option_z_compress" -eq 1 ]; then
		if [ "$g_cmd_compress" = "" ]; then
			throw_usage_error "Compression command (-Z/ZXFER_COMPRESSION) cannot be empty." 2
		fi
		l_compress_tokens=$(split_cli_tokens "$g_cmd_compress")
		if [ "$l_compress_tokens" = "" ]; then
			throw_usage_error "Compression command (-Z/ZXFER_COMPRESSION) cannot be empty." 2
		fi
		if [ "$g_cmd_decompress" = "" ]; then
			throw_error "Compression requested but decompression command missing."
		fi
		l_decompress_tokens=$(split_cli_tokens "$g_cmd_decompress")
		if [ "$l_decompress_tokens" = "" ]; then
			throw_error "Compression requested but decompression command missing."
		fi
		if ! g_cmd_compress_safe=$(zxfer_resolve_local_cli_command_safe "$g_cmd_compress" "compression command"); then
			g_zxfer_failure_class=dependency
			throw_error "$g_cmd_compress_safe"
		fi
		if ! g_cmd_decompress_safe=$(zxfer_resolve_local_cli_command_safe "$g_cmd_decompress" "decompression command"); then
			g_zxfer_failure_class=dependency
			throw_error "$g_cmd_decompress_safe"
		fi
		return
	fi

	g_cmd_compress_safe=$(quote_cli_tokens "$g_cmd_compress")
	g_cmd_decompress_safe=$(quote_cli_tokens "$g_cmd_decompress")
}

zxfer_requote_cli_command_with_resolved_head() {
	l_cli_string=$1
	l_resolved_head=$2
	l_cli_tokens=$(split_cli_tokens "$l_cli_string")
	[ -n "$l_cli_tokens" ] || return 1

	l_output_tokens=""
	l_replaced_head=0

	while IFS= read -r l_cli_token || [ -n "$l_cli_token" ]; do
		[ -n "$l_cli_token" ] || continue
		if [ "$l_replaced_head" -eq 0 ]; then
			l_cli_token=$l_resolved_head
			l_replaced_head=1
		fi
		if [ "$l_output_tokens" = "" ]; then
			l_output_tokens=$l_cli_token
		else
			l_output_tokens="$l_output_tokens
$l_cli_token"
		fi
	done <<-EOF
		$l_cli_tokens
	EOF

	[ "$l_replaced_head" -eq 1 ] || return 1
	quote_token_stream "$l_output_tokens"
}

zxfer_resolve_local_cli_command_safe() {
	l_cli_string=$1
	l_label=${2:-command}
	l_cli_tokens=$(split_cli_tokens "$l_cli_string")
	l_cli_head=$(printf '%s\n' "$l_cli_tokens" | sed -n '1p')
	if [ -z "$l_cli_head" ]; then
		printf '%s\n' "Required dependency \"$l_label\" must not be empty or whitespace-only."
		return 1
	fi

	if ! l_resolved_head=$(zxfer_find_required_tool "$l_cli_head" "$l_label"); then
		printf '%s\n' "$l_resolved_head"
		return 1
	fi

	zxfer_requote_cli_command_with_resolved_head "$l_cli_string" "$l_resolved_head"
}

#
# function that always executes if the script is terminated by a signal
#
trap_exit() {
	# get the exit status of the last command
	l_exit_status=$?

	# Only terminate zxfer-owned background processes. Killing every direct child
	# of the shell is too broad and can clobber coverage helpers or command
	# substitution plumbing in the caller.
	zxfer_kill_registered_cleanup_pids

	if command -v close_all_ssh_control_sockets >/dev/null 2>&1; then
		close_all_ssh_control_sockets
	fi

	# Remove temporary files if they exist
	for l_temp_file in "$g_delete_source_tmp_file" \
		"$g_delete_dest_tmp_file" \
		"$g_delete_snapshots_to_delete_tmp_file"; do
		if [ -f "$l_temp_file" ]; then
			rm "$l_temp_file"
		fi
	done
	if l_tmpdir=$(zxfer_try_get_effective_tmpdir 2>/dev/null); then
		for l_temp_file in "$l_tmpdir/${g_zxfer_temp_prefix:-zxfer.unset}".*; do
			[ -e "$l_temp_file" ] || continue
			rm -rf "$l_temp_file"
		done
	fi
	if [ -n "${g_zxfer_property_cache_dir:-}" ] && [ -d "$g_zxfer_property_cache_dir" ]; then
		rm -rf "$g_zxfer_property_cache_dir"
	fi
	if [ -n "${g_zxfer_snapshot_index_dir:-}" ] && [ -d "$g_zxfer_snapshot_index_dir" ]; then
		rm -rf "$g_zxfer_snapshot_index_dir"
	fi

	if [ "${g_services_need_relaunch:-0}" -eq 1 ]; then
		if [ "${g_services_relaunch_in_progress:-0}" -eq 1 ]; then
			echoV "zxfer exiting with services still stopped after a failed relaunch attempt."
		elif command -v relaunch >/dev/null 2>&1; then
			echoV "zxfer exiting early; restarting stopped services."
			relaunch
		else
			echoV "zxfer exiting with services still stopped; relaunch() unavailable."
		fi
	fi

	echoV "zxfer exiting with status $l_exit_status"
	zxfer_profile_emit_summary
	zxfer_emit_failure_report "$l_exit_status"

	# exit this script
	exit $l_exit_status
}

# catch any signals to terminate the script
# INT (Interrupt) 2 (Ctrl-C)
# TERM (Terminate) 15 (kill)
# HUP (Hangup) 1 (kill -HUP)
# QUIT (Quit) 3 (Ctrl-\)
# EXIT (Exit) 0 (exit)
trap trap_exit INT TERM HUP QUIT EXIT

#
# Check command line parameters.
#
read_command_line_switches() {
	while getopts bBc:dD:eFg:hI:j:kmnN:o:O:PR:sT:UvVwx:YzZ: l_i; do
		case $l_i in
		b)
			g_option_b_beep_always=1
			;;
		B)
			g_option_B_beep_on_success=1
			;;
		c)
			g_option_c_services="$OPTARG"
			;;
		d)
			g_option_d_delete_destination_snapshots=1
			;;
		D)
			g_option_D_display_progress_bar="$OPTARG"
			;;
		e)
			g_option_e_restore_property_mode=1
			# Need to transfer properties, just the backed up properties
			# are substituted
			g_option_P_transfer_property=1
			;;
		F)
			g_option_F_force_rollback="-F"
			;;
		g)
			g_option_g_grandfather_protection="$OPTARG"
			;;
		h)
			usage
			exit 0
			;;
		I)
			g_option_I_ignore_properties="$OPTARG"
			;;
		j)
			# number of parallel jobs and background sends
			g_option_j_jobs="$OPTARG"
			;;
		k)
			g_option_k_backup_property_mode=1
			# In order to back up the properties of the source, the
			# properties of the source must be transferred as well.
			g_option_P_transfer_property=1
			;;
		m)
			g_option_m_migrate=1
			g_option_s_make_snapshot=1
			g_option_P_transfer_property=1
			;;
		n)
			g_option_n_dryrun=1
			;;
		N)
			g_option_N_nonrecursive="$OPTARG"
			;;
		o)
			g_option_o_override_property="$OPTARG"
			;;
		O)
			# since we are using the -O option, we are pulling a remote transfer
			# so we need to use the ssh command to execute the zfs commands
			# $OPTARG is the user@host
			l_new_origin_host="$OPTARG"
			g_option_O_origin_host="$l_new_origin_host"
			refresh_remote_zfs_commands
			;;
		P)
			g_option_P_transfer_property=1
			;;
		R)
			g_option_R_recursive="$OPTARG"
			;;
		s)
			g_option_s_make_snapshot=1
			;;
		T)
			# since we are using the -T option, we are pushing a remote transfer
			# so we need to use the ssh command to execute the zfs commands
			# $OPTARG is the user@host
			l_new_target_host="$OPTARG"
			g_option_T_target_host="$l_new_target_host"
			refresh_remote_zfs_commands
			;;
		U)
			g_option_U_skip_unsupported_properties=1
			;;
		v)
			g_option_v_verbose=1
			;;
		V)
			g_option_v_verbose=1
			g_option_V_very_verbose=1
			;;
		w)
			g_option_w_raw_send=1
			;;
		x)
			g_option_x_exclude_datasets="$OPTARG"
			;;
		Y)
			# set the number of iterations to run through the zfs mode
			g_option_Y_yield_iterations=$g_MAX_YIELD_ITERATIONS
			;;
		z)
			# Pipes the send and receive commands through zstd
			g_option_z_compress=1
			;;
		Z)
			# specify the zstd compression command, like "zstd -T0 -6"
			g_option_z_compress=1
			g_cmd_compress="$OPTARG"
			;;
		\?)
			throw_usage_error "Invalid option provided." 2
			;;
		esac
	done

	refresh_compression_commands
}

# Extract the dataset@snapshot path from a snapshot record. Records may include
# a tab-delimited GUID suffix (`dataset@snap<TAB>guid`) for identity-safe
# comparisons; callers that need the executable ZFS path should use this helper.
extract_snapshot_path() {
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

# Function to extract snapshot name
extract_snapshot_name() {
	l_snapshot_path=$(extract_snapshot_path "$1")

	case "$l_snapshot_path" in
	*@*)
		printf '%s\n' "${l_snapshot_path#*@}"
		;;
	*)
		printf '%s\n' ""
		;;
	esac
}

extract_snapshot_dataset() {
	l_snapshot_path=$(extract_snapshot_path "$1")

	case "$l_snapshot_path" in
	*@*)
		printf '%s\n' "${l_snapshot_path%@*}"
		;;
	*)
		printf '%s\n' ""
		;;
	esac
}

extract_snapshot_guid() {
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

extract_snapshot_identity() {
	l_snapshot_name=$(extract_snapshot_name "$1")
	l_snapshot_guid=$(extract_snapshot_guid "$1")

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

normalize_snapshot_record_list() {
	printf '%s\n' "$1" | tr ' ' '\n'
}

zxfer_snapshot_record_list_contains_guid() {
	l_tab='	'
	case "$1" in
	*"$l_tab"*)
		return 0
		;;
	esac

	return 1
}

zxfer_reverse_snapshot_record_list() {
	l_snapshot_records=$1

	[ -n "$l_snapshot_records" ] || return 0

	# shellcheck disable=SC2016  # awk program should see literal $0/NR.
	printf '%s\n' "$l_snapshot_records" | "${g_cmd_awk:-awk}" '{ l_records[NR] = $0 } END { for (l_i = NR; l_i >= 1; l_i--) if (l_records[l_i] != "") print l_records[l_i] }'
}

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
		normalize_snapshot_record_list "$l_destination_records"
		printf '%s\n' "$l_section_break"
		normalize_snapshot_record_list "$l_source_records"
	} | "${g_cmd_awk:-awk}" -v section_break="$l_section_break" "$l_overlap_awk"
}

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
		normalize_snapshot_record_list "$l_reference_records"
		printf '%s\n' "$l_section_break"
		normalize_snapshot_record_list "$l_identity_records"
	} | "${g_cmd_awk:-awk}" -v section_break="$l_section_break" "$l_filter_awk"
}

zxfer_get_source_snapshot_identity_records_for_dataset() {
	l_dataset=$1

	if ! l_snapshot_records=$(run_source_zfs_cmd list -H -o name,guid -s creation -d 1 -t snapshot "$l_dataset"); then
		return 1
	fi

	l_snapshot_records=$(normalize_snapshot_record_list "$l_snapshot_records")
	zxfer_reverse_snapshot_record_list "$l_snapshot_records"
}

zxfer_get_destination_snapshot_identity_records_for_dataset() {
	l_dataset=$1

	if ! l_snapshot_records=$(run_destination_zfs_cmd list -Hr -o name,guid -t snapshot "$l_dataset"); then
		return 1
	fi

	while IFS= read -r l_snapshot_record; do
		[ -n "$l_snapshot_record" ] || continue
		l_snapshot_path=$(extract_snapshot_path "$l_snapshot_record")
		case "$l_snapshot_path" in
		"$l_dataset"@*)
			printf '%s\n' "$l_snapshot_record"
			;;
		esac
	done <<-EOF
		$(normalize_snapshot_record_list "$l_snapshot_records")
	EOF
}

zxfer_get_snapshot_identity_records_for_dataset() {
	l_side=$1
	l_dataset=$2
	l_reference_records=${3:-}

	case "$l_side" in
	source)
		l_identity_records=$(zxfer_get_source_snapshot_identity_records_for_dataset "$l_dataset") || return 1
		;;
	destination)
		l_identity_records=$(zxfer_get_destination_snapshot_identity_records_for_dataset "$l_dataset") || return 1
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
init_variables() {
	g_origin_cmd_compress_safe=$g_cmd_compress_safe
	g_origin_cmd_decompress_safe=$g_cmd_decompress_safe
	g_target_cmd_compress_safe=$g_cmd_compress_safe
	g_target_cmd_decompress_safe=$g_cmd_decompress_safe

	# determine the source operating system
	if [ "$g_option_O_origin_host" != "" ]; then
		if ! g_source_operating_system=$(get_os "$g_option_O_origin_host" source); then
			g_zxfer_failure_class=dependency
			throw_error "Failed to determine operating system on host $g_option_O_origin_host."
		fi
		if ! g_origin_cmd_zfs=$(resolve_remote_required_tool "$g_option_O_origin_host" zfs "zfs" source); then
			g_zxfer_failure_class=dependency
			throw_error "$g_origin_cmd_zfs"
		fi
		if [ "$g_option_z_compress" -eq 1 ]; then
			if ! g_origin_cmd_compress_safe=$(zxfer_resolve_remote_cli_command_safe "$g_option_O_origin_host" "$g_cmd_compress" "compression command" source); then
				g_zxfer_failure_class=dependency
				throw_error "$g_origin_cmd_compress_safe"
			fi
		fi
	else
		g_source_operating_system=$(get_os "")
		g_origin_cmd_zfs=$g_cmd_zfs
	fi

	# determine the destination operating system
	if [ "$g_option_T_target_host" != "" ]; then
		if ! g_destination_operating_system=$(get_os "$g_option_T_target_host" destination); then
			g_zxfer_failure_class=dependency
			throw_error "Failed to determine operating system on host $g_option_T_target_host."
		fi
		if ! g_target_cmd_zfs=$(resolve_remote_required_tool "$g_option_T_target_host" zfs "zfs" destination); then
			g_zxfer_failure_class=dependency
			throw_error "$g_target_cmd_zfs"
		fi
		if [ "$g_option_z_compress" -eq 1 ]; then
			if ! g_target_cmd_decompress_safe=$(zxfer_resolve_remote_cli_command_safe "$g_option_T_target_host" "$g_cmd_decompress" "decompression command" destination); then
				g_zxfer_failure_class=dependency
				throw_error "$g_target_cmd_decompress_safe"
			fi
		fi
	else
		g_destination_operating_system=$(get_os "")
		g_target_cmd_zfs=$g_cmd_zfs
	fi

	refresh_remote_zfs_commands

	if [ "$g_option_e_restore_property_mode" -eq 1 ]; then
		if [ "$g_option_O_origin_host" = "" ]; then
			zxfer_assign_required_tool g_cmd_cat cat "cat"
		else
			if ! g_cmd_cat=$(resolve_remote_required_tool "$g_option_O_origin_host" cat "cat" source); then
				g_zxfer_failure_class=dependency
				throw_error "$g_cmd_cat"
			fi
		fi
	fi

	l_home_operating_system=$(get_os "")
	if [ "$l_home_operating_system" = "SunOS" ]; then
		l_gawk_path=$(PATH=$g_zxfer_dependency_path command -v gawk 2>/dev/null || :)
		if [ "$l_gawk_path" != "" ]; then
			g_cmd_awk=$l_gawk_path
		fi
	fi
}

#
# Checks that options make sense, etc.
#
consistency_check() {
	# Validate -j early so arithmetic comparisons do not trip /bin/sh errors.
	case ${g_option_j_jobs:-} in
	'' | *[!0-9]*)
		throw_usage_error "The -j option requires a positive integer job count, but received \"${g_option_j_jobs:-}\"."
		;;
	esac
	if [ "$g_option_j_jobs" -le 0 ]; then
		throw_usage_error "The -j option requires a job count of at least 1."
	fi

	# disallow backup and restore of properties at same time
	if [ "$g_option_k_backup_property_mode" -eq 1 ] &&
		[ "$g_option_e_restore_property_mode" -eq 1 ]; then
		throw_usage_error "You cannot bac(k)up and r(e)store properties at the same time."
	fi

	# disallow both beep modes, enforce using one or the other.
	if [ "$g_option_b_beep_always" -eq 1 ] &&
		[ "$g_option_B_beep_on_success" -eq 1 ]; then
		throw_usage_error "You cannot use both beep modes at the same time."
	fi

	if [ "$g_option_z_compress" -eq 1 ] &&
		[ "$g_option_O_origin_host" = "" ] &&
		[ "$g_option_T_target_host" = "" ]; then
		throw_usage_error "-z option can only be used with -O or -T option"
	fi

	if [ "$g_option_g_grandfather_protection" != "" ]; then
		case $g_option_g_grandfather_protection in
		*[!0-9]*)
			throw_usage_error "grandfather protection requires a positive integer; received \"$g_option_g_grandfather_protection\"."
			;;
		*)
			if [ "$g_option_g_grandfather_protection" -le 0 ]; then
				throw_usage_error "grandfather protection requires days greater than 0; received \"$g_option_g_grandfather_protection\"."
			fi
			;;
		esac
	fi

	# disallow migration related options and remote transfers at same time
	if [ "$g_option_T_target_host" != "" ] || [ "$g_option_O_origin_host" != "" ]; then
		if [ "$g_option_m_migrate" -eq 1 ] || [ "$g_option_c_services" != "" ]; then
			throw_usage_error "You cannot migrate to or from a remote host."
		fi
	fi
}
