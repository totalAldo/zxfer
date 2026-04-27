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
# RUNTIME STATE / TEMP FILES / CLEANUP
################################################################################

# Module contract:
# owns globals: per-run option/default state, temp-root selection, runtime-artifact allocation/readback/cleanup state, cleanup PID state, transport/bootstrap defaults, and reporting/profile session state.
# reads globals: TMPDIR, ZXFER_BACKUP_DIR, g_option_* cleanup flags, and resolved helper paths.
# mutates caches: reporting, destination-existence, property, and snapshot-index state through reset helpers.
# returns via stdout: temp-file/temp-dir paths, source-to-destination dataset mappings, and OS detection results.

ZXFER_MAX_YIELD_ITERATIONS=8
ZXFER_CACHE_OBJECT_HEADER_LINE="ZXFER_CACHE_OBJECT_V1"
ZXFER_CACHE_OBJECT_END_LINE="ZXFER_CACHE_OBJECT_END"

# Purpose: Refresh the backup storage root from the current configuration and
# runtime state.
# Usage: Called during runtime bootstrap, staging, and trap cleanup after
# inputs change and downstream helpers need the derived value rebuilt.
zxfer_refresh_backup_storage_root() {
	if [ -n "${ZXFER_BACKUP_DIR:-}" ]; then
		l_backup_storage_root=$ZXFER_BACKUP_DIR
	elif [ -n "${g_backup_storage_root:-}" ]; then
		l_backup_storage_root=$g_backup_storage_root
	elif [ -z "${g_backup_storage_root:-}" ]; then
		l_backup_storage_root=/var/db/zxfer
	fi

	case "$l_backup_storage_root" in
	/*)
		g_backup_storage_root=$l_backup_storage_root
		;;
	*)
		zxfer_throw_error "Refusing to use backup metadata root \"$l_backup_storage_root\" because ZXFER_BACKUP_DIR must be an absolute path."
		;;
	esac
}

# Purpose: Return the destination snapshot root dataset in the form expected by
# later helpers.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_destination_snapshot_root_dataset() {
	l_source_dataset_tail=${g_initial_source##*/}

	if [ "${g_initial_source_had_trailing_slash:-0}" -eq 1 ]; then
		printf '%s\n' "$g_destination"
	else
		printf '%s\n' "$g_destination/$l_source_dataset_tail"
	fi
}

# Purpose: Return the destination dataset for source dataset in the form
# expected by later helpers.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_destination_dataset_for_source_dataset() {
	l_source_dataset=$1
	l_destination_root_dataset=$(zxfer_get_destination_snapshot_root_dataset)

	case "$l_source_dataset" in
	"$g_initial_source")
		printf '%s\n' "$l_destination_root_dataset"
		;;
	"$g_initial_source"/*)
		printf '%s\n' "$l_destination_root_dataset${l_source_dataset#"$g_initial_source"}"
		;;
	*)
		printf '%s\n' "$l_destination_root_dataset"
		;;
	esac
}

zxfer_get_cleanup_child_wrapper_script_path() {
	l_cleanup_child_wrapper_script="${ZXFER_SOURCE_MODULES_ROOT:-.}/src/zxfer_cleanup_child_wrapper.sh"
	[ -r "$l_cleanup_child_wrapper_script" ] || return 1
	printf '%s\n' "$l_cleanup_child_wrapper_script"
}

# Purpose: Reset the validated cleanup-helper tracking state so the next
# runtime pass starts from a clean state.
# Usage: Called during runtime bootstrap, staging, and trap cleanup before this
# module reuses mutable scratch globals or cached decisions.
zxfer_reset_cleanup_pid_tracking() {
	g_zxfer_cleanup_pids=""
	g_zxfer_cleanup_pid_records=""
	g_zxfer_cleanup_pid_record_purpose=""
	g_zxfer_cleanup_pid_record_start_token=""
	g_zxfer_cleanup_pid_record_hostname=""
	g_zxfer_cleanup_pid_record_pgid=""
	g_zxfer_cleanup_pid_record_teardown_mode=""
	g_zxfer_cleanup_pid_abort_failure_message=""
	g_zxfer_cleanup_pid_process_snapshot_result=""
	g_zxfer_cleanup_pid_set_result=""
}

zxfer_validate_cleanup_pid_teardown_mode() {
	case "$1" in
	child_set | process_group)
		return 0
		;;
	esac

	return 1
}

zxfer_read_cleanup_pid_process_group() {
	l_cleanup_read_pgid_pid=$1
	l_cleanup_read_pgid_status=0
	l_cleanup_read_pgid_raw=$("${g_cmd_ps:-ps}" -o pgid= -p "$l_cleanup_read_pgid_pid" 2>/dev/null) ||
		l_cleanup_read_pgid_status=$?
	[ "$l_cleanup_read_pgid_status" -eq 0 ] || return "$l_cleanup_read_pgid_status"
	l_cleanup_read_pgid_status=0
	l_cleanup_read_pgid_value=$(printf '%s\n' "$l_cleanup_read_pgid_raw" |
		sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed -n '1p') ||
		l_cleanup_read_pgid_status=$?
	[ "$l_cleanup_read_pgid_status" -eq 0 ] || return "$l_cleanup_read_pgid_status"

	case "$l_cleanup_read_pgid_value" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	printf '%s\n' "$l_cleanup_read_pgid_value"
}

zxfer_find_cleanup_pid_record() {
	l_cleanup_find_pid=$1

	g_zxfer_cleanup_pid_record_purpose=""
	g_zxfer_cleanup_pid_record_start_token=""
	g_zxfer_cleanup_pid_record_hostname=""
	g_zxfer_cleanup_pid_record_pgid=""
	g_zxfer_cleanup_pid_record_teardown_mode=""

	while IFS='	' read -r l_cleanup_find_record_pid l_cleanup_find_record_purpose l_cleanup_find_record_start_token l_cleanup_find_record_hostname l_cleanup_find_record_pgid l_cleanup_find_record_teardown_mode || [ -n "${l_cleanup_find_record_pid}${l_cleanup_find_record_purpose}${l_cleanup_find_record_start_token}${l_cleanup_find_record_hostname}${l_cleanup_find_record_pgid}${l_cleanup_find_record_teardown_mode}" ]; do
		[ -n "$l_cleanup_find_record_pid" ] || continue
		[ "$l_cleanup_find_record_pid" = "$l_cleanup_find_pid" ] || continue
		g_zxfer_cleanup_pid_record_purpose=$l_cleanup_find_record_purpose
		g_zxfer_cleanup_pid_record_start_token=$l_cleanup_find_record_start_token
		g_zxfer_cleanup_pid_record_hostname=$l_cleanup_find_record_hostname
		g_zxfer_cleanup_pid_record_pgid=$l_cleanup_find_record_pgid
		g_zxfer_cleanup_pid_record_teardown_mode=$l_cleanup_find_record_teardown_mode
		return 0
	done <<-EOF
		${g_zxfer_cleanup_pid_records:-}
	EOF

	return 1
}

# Purpose: Register the cleanup helper with the tracking state owned by this
# module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup so cleanup
# and later lookups can validate the live helper before teardown.
zxfer_register_cleanup_pid() {
	l_cleanup_register_pid=$1
	l_cleanup_register_purpose=${2:-cleanup helper}
	l_cleanup_register_teardown_mode=${3:-child_set}
	l_cleanup_register_had_record=0

	case "$l_cleanup_register_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac
	[ "$l_cleanup_register_pid" = "$$" ] && return 0

	l_cleanup_register_status=0
	l_cleanup_register_purpose=$(zxfer_normalize_owned_lock_text_field "$l_cleanup_register_purpose") ||
		l_cleanup_register_status=$?
	if [ "$l_cleanup_register_status" -ne 0 ]; then
		return "$l_cleanup_register_status"
	fi
	if ! zxfer_validate_cleanup_pid_teardown_mode "$l_cleanup_register_teardown_mode"; then
		return 1
	fi
	if zxfer_find_cleanup_pid_record "$l_cleanup_register_pid"; then
		l_cleanup_register_had_record=1
	fi
	if ! kill -s 0 "$l_cleanup_register_pid" 2>/dev/null; then
		return 0
	fi
	l_cleanup_register_status=0
	l_cleanup_register_start_token=$(zxfer_get_process_start_token "$l_cleanup_register_pid") ||
		l_cleanup_register_status=$?
	if [ "$l_cleanup_register_status" -ne 0 ]; then
		if ! kill -s 0 "$l_cleanup_register_pid" 2>/dev/null; then
			return 0
		fi
		return "$l_cleanup_register_status"
	fi
	l_cleanup_register_status=0
	l_cleanup_register_hostname=$(zxfer_get_owned_lock_hostname) ||
		l_cleanup_register_status=$?
	if [ "$l_cleanup_register_status" -ne 0 ]; then
		if ! kill -s 0 "$l_cleanup_register_pid" 2>/dev/null; then
			return 0
		fi
		return "$l_cleanup_register_status"
	fi
	if [ "$l_cleanup_register_had_record" -eq 1 ]; then
		if [ "$g_zxfer_cleanup_pid_record_start_token" = "$l_cleanup_register_start_token" ] &&
			[ "$g_zxfer_cleanup_pid_record_hostname" = "$l_cleanup_register_hostname" ]; then
			return 0
		fi
		# Replace stale records when a numeric PID has been reused for a new
		# helper before the old record was unregistered.
		zxfer_unregister_cleanup_pid "$l_cleanup_register_pid"
	fi
	l_cleanup_register_pgid=$(zxfer_read_cleanup_pid_process_group "$l_cleanup_register_pid" 2>/dev/null || :)

	if [ -n "${g_zxfer_cleanup_pid_records:-}" ]; then
		g_zxfer_cleanup_pid_records=$g_zxfer_cleanup_pid_records"
$l_cleanup_register_pid	$l_cleanup_register_purpose	$l_cleanup_register_start_token	$l_cleanup_register_hostname	$l_cleanup_register_pgid	$l_cleanup_register_teardown_mode"
	else
		g_zxfer_cleanup_pid_records="$l_cleanup_register_pid	$l_cleanup_register_purpose	$l_cleanup_register_start_token	$l_cleanup_register_hostname	$l_cleanup_register_pgid	$l_cleanup_register_teardown_mode"
	fi
	if [ -n "${g_zxfer_cleanup_pids:-}" ]; then
		g_zxfer_cleanup_pids="$g_zxfer_cleanup_pids $l_cleanup_register_pid"
	else
		g_zxfer_cleanup_pids=$l_cleanup_register_pid
	fi

	return 0
}

# Purpose: Remove the cleanup helper from the tracking state owned by this
# module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup after the
# tracked resource has completed or been cleaned up.
zxfer_unregister_cleanup_pid() {
	l_cleanup_unregister_pid=$1
	l_cleanup_unregister_remaining_pids=""
	l_cleanup_unregister_remaining_records=""

	case "$l_cleanup_unregister_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac

	for l_cleanup_unregister_existing_pid in ${g_zxfer_cleanup_pids:-}; do
		[ "$l_cleanup_unregister_existing_pid" = "$l_cleanup_unregister_pid" ] && continue
		if [ -n "$l_cleanup_unregister_remaining_pids" ]; then
			l_cleanup_unregister_remaining_pids="$l_cleanup_unregister_remaining_pids $l_cleanup_unregister_existing_pid"
		else
			l_cleanup_unregister_remaining_pids=$l_cleanup_unregister_existing_pid
		fi
	done
	while IFS='	' read -r l_cleanup_unregister_record_pid l_cleanup_unregister_record_purpose l_cleanup_unregister_record_start_token l_cleanup_unregister_record_hostname l_cleanup_unregister_record_pgid l_cleanup_unregister_record_teardown_mode || [ -n "${l_cleanup_unregister_record_pid}${l_cleanup_unregister_record_purpose}${l_cleanup_unregister_record_start_token}${l_cleanup_unregister_record_hostname}${l_cleanup_unregister_record_pgid}${l_cleanup_unregister_record_teardown_mode}" ]; do
		[ -n "$l_cleanup_unregister_record_pid" ] || continue
		[ "$l_cleanup_unregister_record_pid" = "$l_cleanup_unregister_pid" ] && continue
		if [ -n "$l_cleanup_unregister_remaining_records" ]; then
			l_cleanup_unregister_remaining_records=$l_cleanup_unregister_remaining_records"
$l_cleanup_unregister_record_pid	$l_cleanup_unregister_record_purpose	$l_cleanup_unregister_record_start_token	$l_cleanup_unregister_record_hostname	$l_cleanup_unregister_record_pgid	$l_cleanup_unregister_record_teardown_mode"
		else
			l_cleanup_unregister_remaining_records="$l_cleanup_unregister_record_pid	$l_cleanup_unregister_record_purpose	$l_cleanup_unregister_record_start_token	$l_cleanup_unregister_record_hostname	$l_cleanup_unregister_record_pgid	$l_cleanup_unregister_record_teardown_mode"
		fi
	done <<-EOF
		${g_zxfer_cleanup_pid_records:-}
	EOF

	g_zxfer_cleanup_pids=$l_cleanup_unregister_remaining_pids
	g_zxfer_cleanup_pid_records=$l_cleanup_unregister_remaining_records
}

zxfer_read_cleanup_pid_process_snapshot() {
	g_zxfer_cleanup_pid_process_snapshot_result=""
	l_cleanup_process_snapshot_status=0
	l_cleanup_process_snapshot=$("${g_cmd_ps:-ps}" -o pid= -o ppid= -o pgid= 2>/dev/null) ||
		l_cleanup_process_snapshot_status=$?
	[ "$l_cleanup_process_snapshot_status" -eq 0 ] || return "$l_cleanup_process_snapshot_status"
	g_zxfer_cleanup_pid_process_snapshot_result=$l_cleanup_process_snapshot
	printf '%s\n' "$l_cleanup_process_snapshot"
}

zxfer_cleanup_pid_snapshot_has_pid_with_pgid() {
	l_cleanup_snapshot_check_snapshot=$1
	l_cleanup_snapshot_check_pid=$2
	l_cleanup_snapshot_check_pgid=$3

	case "$l_cleanup_snapshot_check_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	case "$l_cleanup_snapshot_check_pgid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	# shellcheck disable=SC2016
	printf '%s\n' "$l_cleanup_snapshot_check_snapshot" | "${g_cmd_awk:-awk}" -v want_pid="$l_cleanup_snapshot_check_pid" -v want_pgid="$l_cleanup_snapshot_check_pgid" '
	$1 == want_pid && $3 == want_pgid {
		found = 1
	}
	END {
		exit(found ? 0 : 1)
	}'
}

zxfer_cleanup_pid_snapshot_has_pid_with_parent() {
	l_cleanup_snapshot_parent_check_snapshot=$1
	l_cleanup_snapshot_parent_check_pid=$2
	l_cleanup_snapshot_parent_check_parent_pid=$3

	case "$l_cleanup_snapshot_parent_check_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	case "$l_cleanup_snapshot_parent_check_parent_pid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	# shellcheck disable=SC2016
	printf '%s\n' "$l_cleanup_snapshot_parent_check_snapshot" | "${g_cmd_awk:-awk}" -v want_pid="$l_cleanup_snapshot_parent_check_pid" -v want_parent_pid="$l_cleanup_snapshot_parent_check_parent_pid" '
	$1 == want_pid && $2 == want_parent_pid {
		found = 1
	}
	END {
		exit(found ? 0 : 1)
	}'
}

zxfer_get_cleanup_pid_set() {
	l_cleanup_pid_set_snapshot=$1
	l_cleanup_pid_set_root_pid=$2

	g_zxfer_cleanup_pid_set_result=""
	# shellcheck disable=SC2016
	l_cleanup_pid_set_raw=$(printf '%s\n' "$l_cleanup_pid_set_snapshot" | "${g_cmd_awk:-awk}" -v root="$l_cleanup_pid_set_root_pid" '
	{
		pid = $1
		ppid = $2
		if (pid != "") {
			parent[pid] = ppid
			seen[pid] = 1
		}
	}
	END {
		if (root == "") {
			exit 1
		}
		target[root] = 1
		changed = 1
		while (changed) {
			changed = 0
			for (pid in seen) {
				if ((parent[pid] in target) && !(pid in target)) {
					target[pid] = 1
					changed = 1
				}
			}
		}
		for (pid in target) {
			print pid
		}
	}')
	l_cleanup_pid_set_status=$?
	[ "$l_cleanup_pid_set_status" -eq 0 ] || return "$l_cleanup_pid_set_status"
	l_cleanup_pid_set_status=0
	l_cleanup_pid_set_value=$(printf '%s\n' "$l_cleanup_pid_set_raw" | LC_ALL=C sort -n) ||
		l_cleanup_pid_set_status=$?
	[ "$l_cleanup_pid_set_status" -eq 0 ] || return "$l_cleanup_pid_set_status"
	g_zxfer_cleanup_pid_set_result=$l_cleanup_pid_set_value
	printf '%s\n' "$l_cleanup_pid_set_value"
}

zxfer_signal_cleanup_pid_set() {
	l_cleanup_signal_pid_set=$1
	l_cleanup_signal_name=$2
	l_cleanup_signal_status=0

	while IFS= read -r l_cleanup_signal_pid || [ -n "$l_cleanup_signal_pid" ]; do
		[ -n "$l_cleanup_signal_pid" ] || continue
		kill "-$l_cleanup_signal_name" "$l_cleanup_signal_pid" 2>/dev/null || l_cleanup_signal_status=1
	done <<-EOF
		$l_cleanup_signal_pid_set
	EOF

	return "$l_cleanup_signal_status"
}

zxfer_signal_cleanup_process_group() {
	l_cleanup_signal_pgid=$1
	l_cleanup_signal_name=$2

	case "$l_cleanup_signal_pgid" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	kill "-$l_cleanup_signal_name" "-$l_cleanup_signal_pgid" 2>/dev/null
}

zxfer_abort_direct_child_pid() {
	l_cleanup_direct_abort_pid=$1
	l_cleanup_direct_abort_signal=${2:-TERM}
	l_cleanup_direct_abort_purpose=${3:-cleanup helper}
	l_cleanup_direct_abort_signal_status=0

	g_zxfer_cleanup_pid_abort_failure_message=""
	case "$l_cleanup_direct_abort_pid" in
	'' | *[!0-9]*)
		return 0
		;;
	esac
	[ "$l_cleanup_direct_abort_pid" = "$$" ] && return 1

	l_cleanup_direct_abort_status=0
	l_cleanup_direct_abort_purpose=$(zxfer_normalize_owned_lock_text_field "$l_cleanup_direct_abort_purpose") ||
		l_cleanup_direct_abort_status=$?
	if [ "$l_cleanup_direct_abort_status" -ne 0 ]; then
		return "$l_cleanup_direct_abort_status"
	fi
	if ! kill -s 0 "$l_cleanup_direct_abort_pid" 2>/dev/null; then
		return 0
	fi
	l_cleanup_direct_abort_snapshot_status=0
	zxfer_read_cleanup_pid_process_snapshot >/dev/null || l_cleanup_direct_abort_snapshot_status=$?
	if [ "$l_cleanup_direct_abort_snapshot_status" -ne 0 ]; then
		g_zxfer_cleanup_pid_abort_failure_message="Failed to inspect the process table for cleanup helper [$l_cleanup_direct_abort_purpose] (PID $l_cleanup_direct_abort_pid)."
		return "$l_cleanup_direct_abort_snapshot_status"
	fi
	if ! zxfer_cleanup_pid_snapshot_has_pid_with_parent \
		"$g_zxfer_cleanup_pid_process_snapshot_result" \
		"$l_cleanup_direct_abort_pid" \
		"$$"; then
		if ! kill -s 0 "$l_cleanup_direct_abort_pid" 2>/dev/null; then
			return 0
		fi
		g_zxfer_cleanup_pid_abort_failure_message="Refusing to tear down cleanup helper [$l_cleanup_direct_abort_purpose] (PID $l_cleanup_direct_abort_pid) because it is no longer an owned child of the current zxfer process."
		return 1
	fi
	l_cleanup_direct_abort_status=0
	zxfer_get_cleanup_pid_set \
		"$g_zxfer_cleanup_pid_process_snapshot_result" \
		"$l_cleanup_direct_abort_pid" >/dev/null ||
		l_cleanup_direct_abort_status=$?
	if [ "$l_cleanup_direct_abort_status" -ne 0 ]; then
		g_zxfer_cleanup_pid_abort_failure_message="Failed to derive the owned child set for cleanup helper [$l_cleanup_direct_abort_purpose] (PID $l_cleanup_direct_abort_pid)."
		return "$l_cleanup_direct_abort_status"
	fi
	l_cleanup_direct_abort_pid_set=$g_zxfer_cleanup_pid_set_result
	[ -n "$l_cleanup_direct_abort_pid_set" ] || l_cleanup_direct_abort_pid_set=$l_cleanup_direct_abort_pid
	zxfer_signal_cleanup_pid_set "$l_cleanup_direct_abort_pid_set" "$l_cleanup_direct_abort_signal" || l_cleanup_direct_abort_signal_status=1
	if [ "$l_cleanup_direct_abort_signal_status" -ne 0 ]; then
		if ! kill -s 0 "$l_cleanup_direct_abort_pid" 2>/dev/null; then
			return 0
		fi
		g_zxfer_cleanup_pid_abort_failure_message="Failed to signal the owned child set for cleanup helper [$l_cleanup_direct_abort_purpose] (PID $l_cleanup_direct_abort_pid)."
		return 1
	fi

	return 0
}

zxfer_abort_cleanup_pid() {
	l_cleanup_abort_pid=$1
	l_cleanup_abort_signal=${2:-TERM}
	l_cleanup_abort_signal_status=0
	l_cleanup_abort_target_pid_set=""

	g_zxfer_cleanup_pid_abort_failure_message=""
	if ! zxfer_find_cleanup_pid_record "$l_cleanup_abort_pid"; then
		if [ -n "${g_zxfer_cleanup_pids:-}" ]; then
			case " ${g_zxfer_cleanup_pids:-} " in
			*" $l_cleanup_abort_pid "*)
				g_zxfer_cleanup_pid_abort_failure_message="Refusing to tear down cleanup helper [PID $l_cleanup_abort_pid] because no validated ownership record was found."
				return 1
				;;
			esac
		fi
		return 0
	fi

	l_cleanup_abort_live_status=0
	zxfer_owned_lock_owner_is_live \
		"$l_cleanup_abort_pid" \
		"$g_zxfer_cleanup_pid_record_start_token" \
		"$g_zxfer_cleanup_pid_record_hostname" ||
		l_cleanup_abort_live_status=$?
	case "$l_cleanup_abort_live_status" in
	0)
		:
		;;
	1)
		zxfer_unregister_cleanup_pid "$l_cleanup_abort_pid"
		return 0
		;;
	*)
		g_zxfer_cleanup_pid_abort_failure_message="Failed to validate ownership for cleanup helper [$g_zxfer_cleanup_pid_record_purpose] (PID $l_cleanup_abort_pid)."
		return 1
		;;
	esac

	l_cleanup_abort_snapshot_status=0
	zxfer_read_cleanup_pid_process_snapshot >/dev/null || l_cleanup_abort_snapshot_status=$?
	if [ "$l_cleanup_abort_snapshot_status" -ne 0 ]; then
		g_zxfer_cleanup_pid_abort_failure_message="Failed to inspect the process table for cleanup helper [$g_zxfer_cleanup_pid_record_purpose] (PID $l_cleanup_abort_pid)."
		return "$l_cleanup_abort_snapshot_status"
	fi

	l_cleanup_abort_current_shell_pgid_status=0
	l_cleanup_abort_current_shell_pgid_raw=$("${g_cmd_ps:-ps}" -o pgid= -p "$$" 2>/dev/null) ||
		l_cleanup_abort_current_shell_pgid_status=$?
	if [ "$l_cleanup_abort_current_shell_pgid_status" -eq 0 ]; then
		l_cleanup_abort_current_shell_pgid=$(printf '%s\n' "$l_cleanup_abort_current_shell_pgid_raw" |
			sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed -n '1p') ||
			l_cleanup_abort_current_shell_pgid_status=$?
	else
		l_cleanup_abort_current_shell_pgid=""
	fi
	[ "$l_cleanup_abort_current_shell_pgid_status" -eq 0 ] || l_cleanup_abort_current_shell_pgid=""
	case ${g_zxfer_cleanup_pid_record_pgid:-} in
	'' | *[!0-9]*)
		l_cleanup_abort_use_process_group=0
		;;
	*)
		l_cleanup_abort_use_process_group=0
		if [ "${g_zxfer_cleanup_pid_record_teardown_mode:-}" = "process_group" ] &&
			[ "$g_zxfer_cleanup_pid_record_pgid" != "${l_cleanup_abort_current_shell_pgid:-}" ] &&
			zxfer_cleanup_pid_snapshot_has_pid_with_pgid \
				"$g_zxfer_cleanup_pid_process_snapshot_result" \
				"$l_cleanup_abort_pid" \
				"$g_zxfer_cleanup_pid_record_pgid"; then
			l_cleanup_abort_use_process_group=1
		fi
		;;
	esac

	if [ "$l_cleanup_abort_use_process_group" -eq 1 ]; then
		zxfer_signal_cleanup_process_group "$g_zxfer_cleanup_pid_record_pgid" "$l_cleanup_abort_signal" || l_cleanup_abort_signal_status=1
	else
		l_cleanup_abort_pid_set_status=0
		zxfer_get_cleanup_pid_set \
			"$g_zxfer_cleanup_pid_process_snapshot_result" \
			"$l_cleanup_abort_pid" >/dev/null ||
			l_cleanup_abort_pid_set_status=$?
		if [ "$l_cleanup_abort_pid_set_status" -ne 0 ]; then
			l_cleanup_abort_live_status=0
			zxfer_owned_lock_owner_is_live \
				"$l_cleanup_abort_pid" \
				"$g_zxfer_cleanup_pid_record_start_token" \
				"$g_zxfer_cleanup_pid_record_hostname" ||
				l_cleanup_abort_live_status=$?
			case "$l_cleanup_abort_live_status" in
			1)
				zxfer_unregister_cleanup_pid "$l_cleanup_abort_pid"
				return 0
				;;
			2)
				g_zxfer_cleanup_pid_abort_failure_message="Failed to validate ownership for cleanup helper [$g_zxfer_cleanup_pid_record_purpose] (PID $l_cleanup_abort_pid)."
				return 1
				;;
			esac
			g_zxfer_cleanup_pid_abort_failure_message="Failed to derive the owned child set for cleanup helper [$g_zxfer_cleanup_pid_record_purpose] (PID $l_cleanup_abort_pid)."
			return "$l_cleanup_abort_pid_set_status"
		fi
		l_cleanup_abort_target_pid_set=$g_zxfer_cleanup_pid_set_result
		[ -n "$l_cleanup_abort_target_pid_set" ] || l_cleanup_abort_target_pid_set=$l_cleanup_abort_pid
		zxfer_signal_cleanup_pid_set "$l_cleanup_abort_target_pid_set" "$l_cleanup_abort_signal" || l_cleanup_abort_signal_status=1
	fi

	if [ "$l_cleanup_abort_signal_status" -ne 0 ]; then
		l_cleanup_abort_live_status=0
		zxfer_owned_lock_owner_is_live \
			"$l_cleanup_abort_pid" \
			"$g_zxfer_cleanup_pid_record_start_token" \
			"$g_zxfer_cleanup_pid_record_hostname" ||
			l_cleanup_abort_live_status=$?
		case "$l_cleanup_abort_live_status" in
		1)
			zxfer_unregister_cleanup_pid "$l_cleanup_abort_pid"
			return 0
			;;
		2)
			g_zxfer_cleanup_pid_abort_failure_message="Failed to validate ownership for cleanup helper [$g_zxfer_cleanup_pid_record_purpose] (PID $l_cleanup_abort_pid)."
			return 1
			;;
		esac
		g_zxfer_cleanup_pid_abort_failure_message="Failed to signal the validated teardown target for cleanup helper [$g_zxfer_cleanup_pid_record_purpose] (PID $l_cleanup_abort_pid)."
		return 1
	fi

	zxfer_unregister_cleanup_pid "$l_cleanup_abort_pid"
	return 0
}

# Purpose: Stop the registered cleanup helpers that this module still tracks.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# shutdown or failure handling must stop background work that should not
# survive the current run.
zxfer_kill_registered_cleanup_pids() {
	l_cleanup_kill_abort_status=0
	l_cleanup_kill_first_failure_message=""
	l_cleanup_kill_tracked_pids=${g_zxfer_cleanup_pids:-}
	l_cleanup_kill_remaining_pids=""

	for l_cleanup_kill_pid in $l_cleanup_kill_tracked_pids; do
		case "$l_cleanup_kill_pid" in
		'' | *[!0-9]*)
			continue
			;;
		esac
		[ "$l_cleanup_kill_pid" = "$$" ] && continue
		l_cleanup_kill_status=0
		zxfer_abort_cleanup_pid "$l_cleanup_kill_pid" TERM || l_cleanup_kill_status=$?
		if [ "$l_cleanup_kill_status" -ne 0 ]; then
			[ -n "$l_cleanup_kill_first_failure_message" ] || l_cleanup_kill_first_failure_message=$g_zxfer_cleanup_pid_abort_failure_message
			[ "$l_cleanup_kill_abort_status" -ne 0 ] || l_cleanup_kill_abort_status=$l_cleanup_kill_status
		fi
	done

	if [ "$l_cleanup_kill_abort_status" -eq 0 ]; then
		g_zxfer_cleanup_pid_abort_failure_message=""
	fi
	if [ -n "$l_cleanup_kill_first_failure_message" ]; then
		g_zxfer_cleanup_pid_abort_failure_message=$l_cleanup_kill_first_failure_message
	fi
	while IFS='	' read -r l_cleanup_kill_record_pid l_cleanup_kill_record_purpose l_cleanup_kill_record_start_token l_cleanup_kill_record_hostname l_cleanup_kill_record_pgid l_cleanup_kill_record_teardown_mode || [ -n "${l_cleanup_kill_record_pid}${l_cleanup_kill_record_purpose}${l_cleanup_kill_record_start_token}${l_cleanup_kill_record_hostname}${l_cleanup_kill_record_pgid}${l_cleanup_kill_record_teardown_mode}" ]; do
		[ -n "$l_cleanup_kill_record_pid" ] || continue
		if [ -n "$l_cleanup_kill_remaining_pids" ]; then
			l_cleanup_kill_remaining_pids="$l_cleanup_kill_remaining_pids $l_cleanup_kill_record_pid"
		else
			l_cleanup_kill_remaining_pids=$l_cleanup_kill_record_pid
		fi
	done <<-EOF
		${g_zxfer_cleanup_pid_records:-}
	EOF
	g_zxfer_cleanup_pids=$l_cleanup_kill_remaining_pids

	return "$l_cleanup_kill_abort_status"
}

# Purpose: List the default temporary directory candidates in the stable order
# or format later helpers expect.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when zxfer
# needs a canonical candidate list instead of rebuilding it ad hoc.
zxfer_list_default_tmpdir_candidates() {
	printf '%s\n' "/dev/shm"
	printf '%s\n' "/run/shm"
	printf '%s\n' "/tmp"
}

# Purpose: Try to resolve or create the get default temporary directory without
# treating every miss as fatal.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when zxfer
# has an optional or fallback path that still needs one checked helper.
zxfer_try_get_default_tmpdir() {
	l_candidates=$(zxfer_list_default_tmpdir_candidates)

	while IFS= read -r l_candidate || [ -n "$l_candidate" ]; do
		[ -n "$l_candidate" ] || continue
		if l_effective_tmpdir=$(zxfer_validate_temp_root_candidate "$l_candidate"); then
			printf '%s\n' "$l_effective_tmpdir"
			return 0
		fi
	done <<EOF
$l_candidates
EOF

	return 1
}

# Purpose: Try to resolve or create the get socket cache temporary directory
# without treating every miss as fatal.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when zxfer
# has an optional or fallback path that still needs one checked helper.
zxfer_try_get_socket_cache_tmpdir() {
	l_requested_tmpdir=${TMPDIR:-}

	if [ -n "$l_requested_tmpdir" ] &&
		l_effective_tmpdir=$(zxfer_validate_temp_root_candidate "$l_requested_tmpdir"); then
		case "$l_requested_tmpdir" in
		*/./* | */../* | */. | */..)
			:
			;;
		*)
			if ! zxfer_find_symlink_path_component "$l_requested_tmpdir" >/dev/null 2>&1; then
				printf '%s\n' "$l_requested_tmpdir"
				return 0
			fi
			;;
		esac
	fi

	zxfer_try_get_effective_tmpdir
}

# Purpose: Try to resolve or create the get effective temporary directory
# without treating every miss as fatal.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when zxfer
# has an optional or fallback path that still needs one checked helper.
zxfer_try_get_effective_tmpdir() {
	if [ -n "${TMPDIR:-}" ]; then
		l_requested_tmpdir=$TMPDIR
		l_request_key=$l_requested_tmpdir
	else
		l_requested_tmpdir=""
		l_request_key="__ZXFER_DEFAULT_TMPDIR__"
	fi

	if [ -n "${g_zxfer_effective_tmpdir:-}" ] &&
		[ "${g_zxfer_effective_tmpdir_requested:-}" = "$l_request_key" ]; then
		printf '%s\n' "$g_zxfer_effective_tmpdir"
		return 0
	fi

	if [ -n "$l_requested_tmpdir" ]; then
		if l_effective_tmpdir=$(zxfer_validate_temp_root_candidate "$l_requested_tmpdir"); then
			:
		elif l_effective_tmpdir=$(zxfer_try_get_default_tmpdir); then
			zxfer_echoV "Ignoring unsafe TMPDIR $l_requested_tmpdir; using $l_effective_tmpdir instead."
		else
			l_effective_tmpdir_status=$?
			g_zxfer_effective_tmpdir_requested=$l_request_key
			g_zxfer_effective_tmpdir=""
			return "$l_effective_tmpdir_status"
		fi
	else
		l_effective_tmpdir_status=0
		l_effective_tmpdir=$(zxfer_try_get_default_tmpdir) ||
			l_effective_tmpdir_status=$?
		if [ "$l_effective_tmpdir_status" -ne 0 ]; then
			g_zxfer_effective_tmpdir_requested=$l_request_key
			g_zxfer_effective_tmpdir=""
			return "$l_effective_tmpdir_status"
		fi
	fi

	g_zxfer_effective_tmpdir_requested=$l_request_key
	g_zxfer_effective_tmpdir=$l_effective_tmpdir
	printf '%s\n' "$g_zxfer_effective_tmpdir"
}

# Purpose: Reset the runtime artifact state so the next runtime pass starts
# from a clean state.
# Usage: Called during runtime bootstrap, staging, and trap cleanup before this
# module reuses mutable scratch globals or cached decisions.
zxfer_reset_runtime_artifact_state() {
	if zxfer_cleanup_registered_runtime_artifacts; then
		l_cleanup_status=0
	else
		l_cleanup_status=$?
	fi
	g_zxfer_runtime_artifact_path_result=""
	g_zxfer_runtime_artifact_read_result=""
	return "$l_cleanup_status"
}

# Purpose: Register the runtime artifact path with the tracking state owned by
# this module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup so cleanup
# and later lookups can find the live resource.
zxfer_register_runtime_artifact_path() {
	l_artifact_path=$1

	[ -n "$l_artifact_path" ] || return 0

	while IFS= read -r l_existing_path || [ -n "$l_existing_path" ]; do
		[ -n "$l_existing_path" ] || continue
		[ "$l_existing_path" = "$l_artifact_path" ] && return 0
	done <<EOF
${g_zxfer_runtime_artifact_cleanup_paths:-}
EOF

	if [ -n "${g_zxfer_runtime_artifact_cleanup_paths:-}" ]; then
		g_zxfer_runtime_artifact_cleanup_paths=$g_zxfer_runtime_artifact_cleanup_paths'
'$l_artifact_path
	else
		g_zxfer_runtime_artifact_cleanup_paths=$l_artifact_path
	fi
}

# Purpose: Remove the runtime artifact path from the tracking state owned by
# this module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup after the
# tracked resource has completed or been cleaned up.
zxfer_unregister_runtime_artifact_path() {
	l_artifact_path=$1
	l_remaining_paths=""

	[ -n "$l_artifact_path" ] || return 0

	while IFS= read -r l_existing_path || [ -n "$l_existing_path" ]; do
		[ -n "$l_existing_path" ] || continue
		[ "$l_existing_path" = "$l_artifact_path" ] && continue
		if [ -n "$l_remaining_paths" ]; then
			l_remaining_paths=$l_remaining_paths'
'$l_existing_path
		else
			l_remaining_paths=$l_existing_path
		fi
	done <<EOF
${g_zxfer_runtime_artifact_cleanup_paths:-}
EOF

	g_zxfer_runtime_artifact_cleanup_paths=$l_remaining_paths
}

# Purpose: Clean up the runtime artifact path that this module created or
# tracks.
# Usage: Called during runtime bootstrap, staging, and trap cleanup on success
# and failure paths so temporary state does not linger.
zxfer_cleanup_runtime_artifact_path() {
	l_artifact_path=$1

	[ -n "$l_artifact_path" ] || return 0
	if rm -rf "$l_artifact_path" 2>/dev/null ||
		{ [ ! -e "$l_artifact_path" ] && [ ! -L "$l_artifact_path" ] && [ ! -h "$l_artifact_path" ]; }; then
		zxfer_unregister_runtime_artifact_path "$l_artifact_path"
		return 0
	fi

	return 1
}

# Purpose: Clean up the runtime artifact paths that this module created or
# tracks.
# Usage: Called during runtime bootstrap, staging, and trap cleanup on success
# and failure paths so temporary state does not linger.
zxfer_cleanup_runtime_artifact_paths() {
	l_cleanup_status=0

	for l_artifact_path in "$@"; do
		[ -n "$l_artifact_path" ] || continue
		if ! zxfer_cleanup_runtime_artifact_path "$l_artifact_path"; then
			l_cleanup_status=1
		fi
	done

	return "$l_cleanup_status"
}

# Purpose: Clean up the registered runtime artifacts that this module created
# or tracks.
# Usage: Called during runtime bootstrap, staging, and trap cleanup on success
# and failure paths so temporary state does not linger.
zxfer_cleanup_registered_runtime_artifacts() {
	l_remaining_paths=""

	while IFS= read -r l_artifact_path || [ -n "$l_artifact_path" ]; do
		[ -n "$l_artifact_path" ] || continue
		if rm -rf "$l_artifact_path" 2>/dev/null ||
			{ [ ! -e "$l_artifact_path" ] && [ ! -L "$l_artifact_path" ] && [ ! -h "$l_artifact_path" ]; }; then
			continue
		fi
		if [ -n "$l_remaining_paths" ]; then
			l_remaining_paths=$l_remaining_paths'
'$l_artifact_path
		else
			l_remaining_paths=$l_artifact_path
		fi
	done <<EOF
${g_zxfer_runtime_artifact_cleanup_paths:-}
EOF

	g_zxfer_runtime_artifact_cleanup_paths=$l_remaining_paths
	[ -z "$l_remaining_paths" ]
}

# Purpose: Create the runtime artifact directory using the safety checks owned
# by this module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when zxfer
# needs a fresh staged resource or persistent helper state.
zxfer_create_runtime_artifact_dir() {
	l_prefix=$1

	g_zxfer_runtime_artifact_path_result=""
	l_status=0
	l_tmpdir=$(zxfer_try_get_effective_tmpdir) || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_status=0
	l_artifact_dir=$(mktemp -d "$l_tmpdir/$l_prefix.XXXXXX" 2>/dev/null) || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	zxfer_register_runtime_artifact_path "$l_artifact_dir"
	g_zxfer_runtime_artifact_path_result=$l_artifact_dir
	printf '%s\n' "$l_artifact_dir"
}

# Purpose: Create the runtime artifact file using the safety checks owned by
# this module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when zxfer
# needs a fresh staged resource or persistent helper state.
zxfer_create_runtime_artifact_file() {
	l_prefix=$1

	g_zxfer_runtime_artifact_path_result=""
	l_status=0
	l_tmpdir=$(zxfer_try_get_effective_tmpdir) || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_status=0
	l_artifact_file=$(mktemp "$l_tmpdir/$l_prefix.XXXXXX" 2>/dev/null) || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	zxfer_register_runtime_artifact_path "$l_artifact_file"
	g_zxfer_runtime_artifact_path_result=$l_artifact_file
	printf '%s\n' "$l_artifact_file"
}

# Purpose: Create the runtime artifact file in parent using the safety checks
# owned by this module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when zxfer
# needs a fresh staged resource or persistent helper state.
zxfer_create_runtime_artifact_file_in_parent() {
	l_parent_dir=$1
	l_prefix=${2:-zxfer-runtime-artifact}

	g_zxfer_runtime_artifact_path_result=""
	l_status=0
	l_parent_dir=$(zxfer_validate_temp_root_candidate "$l_parent_dir") || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_status=0
	l_artifact_file=$(mktemp "$l_parent_dir/$l_prefix.XXXXXX" 2>/dev/null) || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	zxfer_register_runtime_artifact_path "$l_artifact_file"
	g_zxfer_runtime_artifact_path_result=$l_artifact_file
	printf '%s\n' "$l_artifact_file"
}

# Purpose: Stage the runtime artifact file for path in temporary state before
# it becomes live.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when the
# module needs a same-run scratch artifact or pre-commit staging path.
zxfer_stage_runtime_artifact_file_for_path() {
	l_target_path=$1
	l_prefix=${2:-zxfer-runtime-stage}

	g_zxfer_runtime_artifact_path_result=""
	l_status=0
	l_parent_dir=$(zxfer_get_path_parent_dir "$l_target_path") || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi

	zxfer_create_runtime_artifact_file_in_parent "$l_parent_dir" ".$l_prefix"
}

# Purpose: Write the runtime artifact file in the normalized form later zxfer
# steps expect.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when the
# module needs a stable staged file or emitted stream for downstream use.
zxfer_write_runtime_artifact_file() {
	l_artifact_path=$1
	l_artifact_payload=$2

	[ -n "$l_artifact_path" ] || return 1
	if (
		printf '%s' "$l_artifact_payload" >"$l_artifact_path"
	) 2>/dev/null; then
		return 0
	else
		l_status=$?
	fi

	case "$l_status" in
	1 | 2)
		# dash reports redirection-open failures as status 2 while other
		# supported /bin/sh implementations collapse the same failure to 1.
		return 1
		;;
	esac

	return "$l_status"
}

# Purpose: Read the runtime artifact file from staged state into the current
# shell.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when later
# helpers need a checked reload instead of ad hoc file reads.
zxfer_read_runtime_artifact_file() {
	l_artifact_path=$1
	l_artifact_contents=""

	g_zxfer_runtime_artifact_read_result=""
	[ -r "$l_artifact_path" ] || return 1

	l_read_status=0
	l_artifact_contents=$(
		cat "$l_artifact_path"
		l_read_status=$?
		# Keep one non-newline sentinel inside the substitution so trailing
		# blank lines from the artifact survive command substitution intact.
		printf x
		exit "$l_read_status"
	) || l_read_status=$?
	if [ "$l_read_status" -ne 0 ]; then
		return "$l_read_status"
	fi
	l_artifact_contents=${l_artifact_contents%?}

	g_zxfer_runtime_artifact_read_result=$l_artifact_contents
	printf '%s' "$l_artifact_contents"
}

# Purpose: Publish the runtime artifact file from staged state to its live
# destination.
# Usage: Called during runtime bootstrap, staging, and trap cleanup after
# staged validation succeeds and the result is ready to replace the live
# object.
zxfer_publish_runtime_artifact_file() {
	l_stage_file=$1
	l_target_path=$2

	[ -n "$l_stage_file" ] || return 1
	[ -n "$l_target_path" ] || return 1
	l_status=0
	mv -f "$l_stage_file" "$l_target_path" 2>/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	zxfer_unregister_runtime_artifact_path "$l_stage_file"
	return 0
}

# Purpose: Write the runtime cache file atomically in the normalized form later
# zxfer steps expect.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when the
# module needs a stable staged file or emitted stream for downstream use.
zxfer_write_runtime_cache_file_atomically() {
	l_target_path=$1
	l_cache_payload=$2
	l_prefix=${3:-zxfer-runtime-cache}

	[ -n "$l_target_path" ] || return 1
	[ ! -L "$l_target_path" ] || return 1
	[ ! -h "$l_target_path" ] || return 1
	if [ -e "$l_target_path" ]; then
		[ -f "$l_target_path" ] || return 1
	fi

	l_status=0
	l_parent_dir=$(zxfer_get_path_parent_dir "$l_target_path") || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	if [ ! -d "$l_parent_dir" ]; then
		return 1
	fi
	l_status=0
	zxfer_stage_runtime_artifact_file_for_path "$l_target_path" "$l_prefix" >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_stage_file=$g_zxfer_runtime_artifact_path_result

	l_status=0
	zxfer_write_runtime_artifact_file "$l_stage_file" "$l_cache_payload" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_stage_file"
		return "$l_status"
	fi

	chmod 600 "$l_stage_file" 2>/dev/null || :
	l_status=0
	zxfer_publish_runtime_artifact_file "$l_stage_file" "$l_target_path" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_path "$l_stage_file"
		return "$l_status"
	fi
	chmod 600 "$l_target_path" 2>/dev/null || :
	return 0
}

# Purpose: Create the private temp directory using the safety checks owned by
# this module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when zxfer
# needs a fresh staged resource or persistent helper state.
zxfer_create_private_temp_dir() {
	l_prefix=$1

	l_status=0
	zxfer_create_runtime_artifact_dir "$l_prefix" >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi

	printf '%s\n' "$g_zxfer_runtime_artifact_path_result"
}

# Purpose: Return the temp file in the form expected by later helpers.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_temp_file() {
	g_zxfer_temp_file_result=""
	# On GNU mktemp the template must include X, so build the template ourselves.
	l_prefix=${g_zxfer_temp_prefix:-zxfer.$$.${g_option_Y_yield_iterations:-1}.$(date +%s)}
	l_status=0
	zxfer_create_runtime_artifact_file "$l_prefix" >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_throw_error "Error creating temporary file." "$l_status"
	fi
	zxfer_echoV "New temporary file: $g_zxfer_runtime_artifact_path_result"
	g_zxfer_temp_file_result=$g_zxfer_runtime_artifact_path_result

	# return the temp file name
	echo "$g_zxfer_temp_file_result"
}

# Purpose: Reset the cache object result state so the next runtime pass starts
# from a clean state.
# Usage: Called during runtime bootstrap, staging, and trap cleanup before this
# module reuses mutable scratch globals or cached decisions.
zxfer_reset_cache_object_result_state() {
	g_zxfer_cache_object_kind_result=""
	g_zxfer_cache_object_metadata_result=""
	g_zxfer_cache_object_payload_result=""
}

# Purpose: Validate the cache object metadata lines before zxfer relies on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup to fail
# closed on malformed, unsafe, or stale input.
zxfer_validate_cache_object_metadata_lines() {
	l_metadata=$1

	[ -n "$l_metadata" ] || return 0

	while IFS= read -r l_line || [ -n "$l_line" ]; do
		case "$l_line" in
		*=*)
			[ -n "${l_line%%=*}" ] || return 1
			;;
		*)
			return 1
			;;
		esac
	done <<-EOF
		$l_metadata
	EOF

	return 0
}

# Purpose: Return the cache object metadata value in the form expected by later
# helpers.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_cache_object_metadata_value() {
	l_metadata=$1
	l_key=$2

	[ -n "$l_key" ] || return 1
	[ -n "$l_metadata" ] || return 1

	while IFS= read -r l_line || [ -n "$l_line" ]; do
		case "$l_line" in
		"$l_key"=*)
			printf '%s\n' "${l_line#"$l_key"=}"
			return 0
			;;
		esac
	done <<-EOF
		$l_metadata
	EOF

	return 1
}

# Purpose: Create the cache object stage directory in parent using the safety
# checks owned by this module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when zxfer
# needs a fresh staged resource or persistent helper state.
zxfer_create_cache_object_stage_dir_in_parent() {
	l_parent_dir=$1
	l_prefix=${2:-zxfer-cache-object}

	g_zxfer_runtime_artifact_path_result=""
	l_status=0
	l_parent_dir=$(zxfer_validate_temp_root_candidate "$l_parent_dir") || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi

	l_old_umask=$(umask)
	umask 077
	l_stage_status=0
	l_stage_dir=$(mktemp -d "$l_parent_dir/.$l_prefix.XXXXXX" 2>/dev/null) ||
		l_stage_status=$?
	umask "$l_old_umask"
	[ "$l_stage_status" -eq 0 ] || return "$l_stage_status"

	zxfer_register_runtime_artifact_path "$l_stage_dir"
	g_zxfer_runtime_artifact_path_result=$l_stage_dir
	printf '%s\n' "$l_stage_dir"
}

# Purpose: Create the cache object stage directory for path using the safety
# checks owned by this module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when zxfer
# needs a fresh staged resource or persistent helper state.
zxfer_create_cache_object_stage_dir_for_path() {
	l_object_path=$1
	l_prefix=${2:-zxfer-cache-object}

	l_status=0
	l_parent_dir=$(zxfer_get_path_parent_dir "$l_object_path") || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi

	zxfer_create_cache_object_stage_dir_in_parent "$l_parent_dir" "$l_prefix"
}

# Purpose: Clean up the cache object stage directory that this module created
# or tracks.
# Usage: Called during runtime bootstrap, staging, and trap cleanup on success
# and failure paths so temporary state does not linger.
zxfer_cleanup_cache_object_stage_dir() {
	l_stage_dir=$1

	zxfer_cleanup_runtime_artifact_path "$l_stage_dir"
}

# Purpose: Write the cache object contents to path in the normalized form later
# zxfer steps expect.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when the
# module needs a stable staged file or emitted stream for downstream use.
zxfer_write_cache_object_contents_to_path() {
	l_object_path=$1
	l_object_kind=$2
	l_object_metadata=$3
	l_object_payload=$4

	[ -n "$l_object_path" ] || return 1
	[ ! -L "$l_object_path" ] || return 1
	[ ! -h "$l_object_path" ] || return 1
	[ -n "$l_object_kind" ] || return 1
	[ -n "$l_object_payload" ] || return 1
	l_object_write_status=0
	zxfer_validate_cache_object_metadata_lines "$l_object_metadata" ||
		l_object_write_status=$?
	if [ "$l_object_write_status" -ne 0 ]; then
		return "$l_object_write_status"
	fi

	l_old_umask=$(umask)
	umask 077
	l_object_write_status=0
	{
		printf '%s\n' "$ZXFER_CACHE_OBJECT_HEADER_LINE"
		printf 'kind=%s\n' "$l_object_kind"
		[ -z "$l_object_metadata" ] || printf '%s\n' "$l_object_metadata"
		printf '\n'
		printf '%s' "$l_object_payload"
		printf '\n%s\n' "$ZXFER_CACHE_OBJECT_END_LINE"
	} >"$l_object_path" || l_object_write_status=$?
	if [ "$l_object_write_status" -ne 0 ]; then
		umask "$l_old_umask"
		rm -f "$l_object_path" 2>/dev/null || :
		return "$l_object_write_status"
	fi
	umask "$l_old_umask"

	chmod 600 "$l_object_path" 2>/dev/null || :
	return 0
}

# Purpose: Read the cache object file from staged state into the current shell.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when later
# helpers need a checked reload instead of ad hoc file reads.
zxfer_read_cache_object_file() {
	l_object_path=$1
	l_expected_kind=$2
	l_object_contents=""

	zxfer_reset_cache_object_result_state

	[ -f "$l_object_path" ] || return 1
	[ ! -L "$l_object_path" ] || return 1
	[ ! -h "$l_object_path" ] || return 1
	if zxfer_read_runtime_artifact_file "$l_object_path" >/dev/null; then
		l_object_contents=$g_zxfer_runtime_artifact_read_result
	else
		l_read_status=$?
		return "$l_read_status"
	fi
	case "$l_object_contents" in
	*'
')
		l_object_contents=${l_object_contents%?}
		;;
	esac

	l_line_number=0
	l_separator_seen=0
	l_object_kind=""
	l_object_metadata=""
	l_object_payload=""
	l_object_payload_has_lines=0
	l_previous_payload_line=""
	l_previous_payload_line_set=0

	while IFS= read -r l_line || [ -n "$l_line" ]; do
		l_line_number=$((l_line_number + 1))
		case "$l_line_number" in
		1)
			[ "$l_line" = "$ZXFER_CACHE_OBJECT_HEADER_LINE" ] || return 1
			continue
			;;
		2)
			case "$l_line" in
			kind=*)
				l_object_kind=${l_line#kind=}
				;;
			*)
				return 1
				;;
			esac
			[ -n "$l_object_kind" ] || return 1
			[ -z "$l_expected_kind" ] || [ "$l_object_kind" = "$l_expected_kind" ] || return 1
			continue
			;;
		esac

		if [ "$l_separator_seen" -eq 0 ]; then
			if [ "$l_line" = "" ]; then
				l_separator_seen=1
				continue
			fi

			case "$l_line" in
			*=*)
				[ -n "${l_line%%=*}" ] || return 1
				if [ -n "$l_object_metadata" ]; then
					l_object_metadata="$l_object_metadata
$l_line"
				else
					l_object_metadata=$l_line
				fi
				;;
			*)
				return 1
				;;
			esac
			continue
		fi

		if [ "$l_previous_payload_line_set" -eq 1 ]; then
			if [ "$l_object_payload_has_lines" -eq 1 ]; then
				l_object_payload="$l_object_payload
$l_previous_payload_line"
			else
				l_object_payload=$l_previous_payload_line
				l_object_payload_has_lines=1
			fi
		fi

		l_previous_payload_line=$l_line
		l_previous_payload_line_set=1
	done <<EOF
$l_object_contents
EOF

	[ "$l_line_number" -ge 5 ] || return 1
	[ "$l_separator_seen" -eq 1 ] || return 1
	[ "$l_previous_payload_line_set" -eq 1 ] || return 1
	[ "$l_previous_payload_line" = "$ZXFER_CACHE_OBJECT_END_LINE" ] || return 1
	[ "$l_object_payload_has_lines" -eq 1 ] || return 1
	[ -n "$l_object_payload" ] || return 1

	g_zxfer_cache_object_kind_result=$l_object_kind
	g_zxfer_cache_object_metadata_result=$l_object_metadata
	g_zxfer_cache_object_payload_result=$l_object_payload
	printf '%s\n' "$l_object_payload"
}

# Purpose: Write the cache object file atomically in the normalized form later
# zxfer steps expect.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when the
# module needs a stable staged file or emitted stream for downstream use.
zxfer_write_cache_object_file_atomically() {
	l_cache_target_path=$1
	l_cache_object_kind=$2
	l_cache_object_metadata=$3
	l_cache_object_payload=$4
	l_cache_stage_dir=""
	l_cache_stage_file=""

	[ -n "$l_cache_target_path" ] || return 1
	[ ! -L "$l_cache_target_path" ] || return 1
	[ ! -h "$l_cache_target_path" ] || return 1
	if [ -e "$l_cache_target_path" ]; then
		[ -f "$l_cache_target_path" ] || return 1
	fi

	l_status=0
	l_cache_parent_dir=$(zxfer_get_path_parent_dir "$l_cache_target_path") || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_status=0
	mkdir -p "$l_cache_parent_dir" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_status=0
	zxfer_create_cache_object_stage_dir_for_path \
		"$l_cache_target_path" "zxfer-cache-object" >/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	l_cache_stage_dir=$g_zxfer_runtime_artifact_path_result
	l_cache_stage_file="$l_cache_stage_dir/object"

	l_status=0
	zxfer_write_cache_object_contents_to_path \
		"$l_cache_stage_file" \
		"$l_cache_object_kind" \
		"$l_cache_object_metadata" \
		"$l_cache_object_payload" || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_cache_object_stage_dir "$l_cache_stage_dir"
		return "$l_status"
	fi
	l_status=0
	zxfer_read_cache_object_file \
		"$l_cache_stage_file" "$l_cache_object_kind" >/dev/null 2>&1 || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_cache_object_stage_dir "$l_cache_stage_dir"
		return "$l_status"
	fi
	l_status=0
	mv -f "$l_cache_stage_file" "$l_cache_target_path" 2>/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		zxfer_cleanup_cache_object_stage_dir "$l_cache_stage_dir"
		return "$l_status"
	fi
	chmod 600 "$l_cache_target_path" 2>/dev/null || :
	zxfer_cleanup_cache_object_stage_dir "$l_cache_stage_dir"
	return 0
}

# Purpose: Publish the cache object directory from staged state to its live
# destination.
# Usage: Called during runtime bootstrap, staging, and trap cleanup after
# staged validation succeeds and the result is ready to replace the live
# object.
zxfer_publish_cache_object_directory() {
	l_stage_dir=$1
	l_object_dir=$2

	[ -n "$l_stage_dir" ] || return 1
	[ -d "$l_stage_dir" ] || return 1
	[ ! -L "$l_stage_dir" ] || return 1
	[ ! -h "$l_stage_dir" ] || return 1
	[ -n "$l_object_dir" ] || return 1
	[ ! -e "$l_object_dir" ] || return 1

	l_status=0
	l_parent_dir=$(zxfer_get_path_parent_dir "$l_object_dir") || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	if [ ! -d "$l_parent_dir" ]; then
		return 1
	fi
	l_status=0
	l_parent_dir=$(zxfer_validate_temp_root_candidate "$l_parent_dir") || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi

	l_status=0
	mv "$l_stage_dir" "$l_object_dir" 2>/dev/null || l_status=$?
	if [ "$l_status" -ne 0 ]; then
		return "$l_status"
	fi
	zxfer_unregister_runtime_artifact_path "$l_stage_dir"
	return 0
}

# Purpose: Return the operating system in the form expected by later helpers.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# sibling helpers need the same lookup without duplicating module logic.
#
# Gets a $(uname), i.e. the operating system, for origin or target, if remote.
# Takes: $1=either $g_option_O_origin_host or $g_option_T_target_host
zxfer_get_os() {
	l_host_spec=$1
	l_profile_side=${2:-}
	l_output_os=""

	# Get uname of the destination (target) machine, local or remote
	if [ "$l_host_spec" = "" ]; then
		l_output_os=$(uname)
	else
		l_os_status=0
		l_output_os=$(zxfer_get_remote_host_operating_system "$l_host_spec" "$l_profile_side") ||
			l_os_status=$?
		if [ "$l_os_status" -ne 0 ]; then
			return "$l_os_status"
		fi
	fi

	echo "$l_output_os"
}

# Purpose: Return the max yield iterations in the form expected by later
# helpers.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when
# sibling helpers need the same lookup without duplicating module logic.
zxfer_get_max_yield_iterations() {
	printf '%s\n' "$ZXFER_MAX_YIELD_ITERATIONS"
}

# Purpose: Initialize the runtime metadata before later helpers depend on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_runtime_metadata() {
	# zxfer version
	g_zxfer_version="2.0.0-20260423"
}

# Purpose: Initialize the option defaults before later helpers depend on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_option_defaults() {
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
}

# Purpose: Initialize the dependency tool defaults before later helpers depend
# on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_dependency_tool_defaults() {
	g_cmd_zfs=""
	g_cmd_ssh=""

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
	g_cmd_ps=""

	zxfer_assign_required_tool g_cmd_awk awk "awk"
	zxfer_assign_required_tool g_cmd_zfs zfs "zfs"
	g_cmd_parallel=$(PATH=$g_zxfer_dependency_path command -v parallel 2>/dev/null || :)
	if [ "$g_cmd_parallel" != "" ]; then
		l_dependency_status=0
		g_cmd_parallel=$(zxfer_validate_resolved_tool_path "$g_cmd_parallel" "GNU parallel") ||
			l_dependency_status=$?
		if [ "$l_dependency_status" -ne 0 ]; then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "$g_cmd_parallel" "$l_dependency_status"
		fi
	fi

	# enable compression in ssh options so that remote snapshot lists that
	# contain thousands of snapshots are compressed
	zxfer_assign_required_tool g_cmd_ps ps "ps"
	zxfer_refresh_compression_commands
}

# Purpose: Refresh the ssh control-socket capability flag from the current
# local ssh helper state.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap and later lazy ssh resolution so downstream code can rely on one
# cached support flag.
zxfer_refresh_ssh_control_socket_support_state() {
	g_ssh_supports_control_sockets=0
	if zxfer_ssh_supports_control_sockets; then
		g_ssh_supports_control_sockets=1
	fi
}

# Purpose: Initialize the transport remote defaults before later helpers depend
# on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_transport_remote_defaults() {
	g_origin_remote_capabilities_host=""
	g_origin_remote_capabilities_dependency_path=""
	g_origin_remote_capabilities_cache_identity=""
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_bootstrap_source=""
	g_origin_remote_capabilities_cache_write_unavailable=0
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_dependency_path=""
	g_target_remote_capabilities_cache_identity=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_cache_write_unavailable=0
	g_zxfer_remote_capability_response_result=""
	g_zxfer_remote_capability_tool_records=""
	g_zxfer_remote_capability_tool_status_result=""
	g_zxfer_remote_capability_tool_path_result=""
	g_zxfer_remote_capability_requested_tools_result=""
	g_zxfer_remote_probe_stdout=""
	g_zxfer_remote_probe_stderr=""
	g_zxfer_remote_probe_capture_read_result=""
	g_zxfer_remote_probe_capture_failed=0
	g_zxfer_ssh_control_socket_action_result=""
	g_zxfer_ssh_control_socket_action_stderr=""
	g_zxfer_ssh_control_socket_action_command=""
	g_zxfer_ssh_control_socket_lock_dir_result=""
	g_zxfer_ssh_control_socket_lock_error=""
	g_zxfer_ssh_control_socket_lease_count_result=""
	g_zxfer_remote_capability_cache_ttl=15
	g_zxfer_remote_capability_cache_wait_retries=5
	g_source_operating_system=""
	g_destination_operating_system=""
	g_origin_parallel_cmd=""
	g_origin_parallel_cmd_host=""
	g_zxfer_parallel_source_job_check_kind=""

	# ssh control sockets used for origin (-O) and target (-T) hosts
	g_ssh_origin_control_socket=""
	g_ssh_origin_control_socket_dir=""
	g_ssh_origin_control_socket_lease_file=""
	g_ssh_target_control_socket=""
	g_ssh_target_control_socket_dir=""
	g_ssh_target_control_socket_lease_file=""
	zxfer_refresh_ssh_control_socket_support_state

	# default zfs commands, can be overridden by -O or -T
	g_LZFS=$g_cmd_zfs
	g_RZFS=$g_cmd_zfs
	g_origin_cmd_zfs=$g_cmd_zfs
	g_target_cmd_zfs=$g_cmd_zfs
}

# Purpose: Initialize the runtime state defaults before later helpers depend on
# it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_runtime_state_defaults() {
	g_zxfer_new_snapshot_name=zxfer_$$_$(date +%Y%m%d%H%M%S)

	# profiling and session-scoped scratch state
	zxfer_reset_cleanup_pid_tracking
	g_zxfer_effective_tmpdir=""
	g_zxfer_effective_tmpdir_requested=""
	g_zxfer_temp_file_result=""
	if command -v zxfer_reset_owned_lock_tracking >/dev/null 2>&1; then
		zxfer_reset_owned_lock_tracking
	fi
	zxfer_reset_runtime_artifact_state
	g_zxfer_profile_start_epoch=$(date '+%s' 2>/dev/null || :)
	if ! g_zxfer_profile_start_ms=$(zxfer_profile_now_ms 2>/dev/null); then
		g_zxfer_profile_start_ms=""
	fi
	g_zxfer_profile_has_data=0
	g_zxfer_profile_summary_emitted=0
	g_zxfer_profile_startup_latency_ms=0
	g_zxfer_profile_startup_latency_recorded=0
	g_zxfer_profile_cleanup_ms=0
	g_zxfer_profile_ssh_setup_ms=0
	g_zxfer_profile_source_snapshot_listing_ms=0
	g_zxfer_profile_destination_snapshot_listing_ms=0
	g_zxfer_profile_snapshot_diff_sort_ms=0
	g_zxfer_profile_ssh_control_socket_lock_wait_count=0
	g_zxfer_profile_ssh_control_socket_lock_wait_ms=0
	g_zxfer_profile_remote_capability_cache_wait_count=0
	g_zxfer_profile_remote_capability_cache_wait_ms=0
	g_zxfer_profile_remote_capability_bootstrap_live=0
	g_zxfer_profile_remote_capability_bootstrap_cache=0
	g_zxfer_profile_remote_capability_bootstrap_memory=0
	g_zxfer_profile_remote_cli_tool_direct_probes=0
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
	zxfer_reset_cache_object_result_state
	g_destination=""
	zxfer_refresh_backup_storage_root

	g_ensure_writable=0 # when creating/setting properties, ensures readonly=off
	g_backup_file_extension=".zxfer_backup_info"
}

# Purpose: Reset the delete temp artifacts so the next runtime pass starts from
# a clean state.
# Usage: Called during runtime bootstrap, staging, and trap cleanup before this
# module reuses mutable scratch globals or cached decisions.
zxfer_reset_delete_temp_artifacts() {
	g_delete_source_tmp_file=""
	g_delete_dest_tmp_file=""
	g_delete_snapshots_to_delete_tmp_file=""
}

# Purpose: Initialize the temp artifacts before later helpers depend on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_temp_artifacts() {
	g_zxfer_temp_prefix="zxfer.$$.${g_option_Y_yield_iterations}.$(date +%s)"
	zxfer_reset_delete_temp_artifacts
}

# Purpose: Ensure the snapshot delete temp artifacts exists and is ready before
# the flow continues.
# Usage: Called during runtime bootstrap, staging, and trap cleanup before
# later helpers assume the resource or cache is available.
zxfer_ensure_snapshot_delete_temp_artifacts() {
	l_delete_source_tmp_file=${g_delete_source_tmp_file:-}
	l_delete_dest_tmp_file=${g_delete_dest_tmp_file:-}
	l_delete_snapshots_to_delete_tmp_file=${g_delete_snapshots_to_delete_tmp_file:-}
	l_new_delete_source_tmp_file=""
	l_new_delete_dest_tmp_file=""

	if [ -z "$l_delete_source_tmp_file" ]; then
		if zxfer_get_temp_file >/dev/null; then
			:
		else
			l_status=$?
			return "$l_status"
		fi
		l_delete_source_tmp_file=$g_zxfer_temp_file_result
		l_new_delete_source_tmp_file=$l_delete_source_tmp_file
	fi

	if [ -z "$l_delete_dest_tmp_file" ]; then
		if zxfer_get_temp_file >/dev/null; then
			:
		else
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths \
				"$l_new_delete_source_tmp_file"
			return "$l_status"
		fi
		l_delete_dest_tmp_file=$g_zxfer_temp_file_result
		l_new_delete_dest_tmp_file=$l_delete_dest_tmp_file
	fi

	if [ -z "$l_delete_snapshots_to_delete_tmp_file" ]; then
		if zxfer_get_temp_file >/dev/null; then
			:
		else
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths \
				"$l_new_delete_source_tmp_file" \
				"$l_new_delete_dest_tmp_file"
			return "$l_status"
		fi
		l_delete_snapshots_to_delete_tmp_file=$g_zxfer_temp_file_result
	fi

	g_delete_source_tmp_file=$l_delete_source_tmp_file
	g_delete_dest_tmp_file=$l_delete_dest_tmp_file
	g_delete_snapshots_to_delete_tmp_file=$l_delete_snapshots_to_delete_tmp_file
	return 0
}

# Purpose: Initialize the globals before later helpers depend on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_globals() {
	zxfer_reset_failure_context "startup"
	zxfer_refresh_secure_path_state

	zxfer_init_runtime_metadata
	zxfer_init_option_defaults
	zxfer_init_runtime_state_defaults
	if command -v zxfer_reset_replication_runtime_state >/dev/null 2>&1; then
		zxfer_reset_replication_runtime_state
	fi
	if command -v zxfer_reset_send_receive_state >/dev/null 2>&1; then
		zxfer_reset_send_receive_state
	fi
	if command -v zxfer_reset_background_job_state >/dev/null 2>&1; then
		zxfer_reset_background_job_state
	fi
	if command -v zxfer_reset_destination_existence_cache >/dev/null 2>&1; then
		zxfer_reset_destination_existence_cache
	fi
	if command -v zxfer_reset_snapshot_record_indexes >/dev/null 2>&1; then
		zxfer_reset_snapshot_record_indexes
	fi
	if command -v zxfer_reset_snapshot_discovery_state >/dev/null 2>&1; then
		zxfer_reset_snapshot_discovery_state
	fi
	if command -v zxfer_reset_snapshot_reconcile_state >/dev/null 2>&1; then
		zxfer_reset_snapshot_reconcile_state
	fi
	if command -v zxfer_reset_backup_metadata_state >/dev/null 2>&1; then
		zxfer_reset_backup_metadata_state
	fi
	if command -v zxfer_reset_property_runtime_state >/dev/null 2>&1; then
		zxfer_reset_property_runtime_state
	fi
	# Property scratch state lives with the property modules; reset it through
	# their public helpers so startup and iteration resets cannot drift apart.
	if command -v zxfer_reset_property_iteration_caches >/dev/null 2>&1; then
		zxfer_reset_property_iteration_caches
	fi
	if command -v zxfer_reset_property_reconcile_state >/dev/null 2>&1; then
		zxfer_reset_property_reconcile_state
	fi
	zxfer_init_dependency_tool_defaults
	zxfer_init_transport_remote_defaults
	zxfer_init_temp_artifacts
	zxfer_apply_secure_path
}

# Purpose: Run the centralized shutdown path that cleans up runtime artifacts,
# transports, and end-of-run reporting state.
# Usage: Called during runtime bootstrap, staging, and trap cleanup when the
# shell exits so cleanup and failure reporting stay consistent across success
# and failure paths.
zxfer_trap_exit() {
	# get the exit status of the last command
	l_exit_status=$?
	l_cleanup_start_ms=$(zxfer_profile_now_ms 2>/dev/null || :)

	# Only terminate zxfer-owned background processes. Killing every direct child
	# of the shell is too broad and can clobber coverage helpers or command
	# substitution plumbing in the caller.
	if command -v zxfer_abort_all_background_jobs >/dev/null 2>&1; then
		l_background_cleanup_status=0
		zxfer_abort_all_background_jobs || l_background_cleanup_status=$?
		if [ "$l_background_cleanup_status" -ne 0 ]; then
			[ "$l_exit_status" -eq 0 ] && l_exit_status=$l_background_cleanup_status
			if [ -z "${g_zxfer_failure_message:-}" ]; then
				g_zxfer_failure_class=runtime
				g_zxfer_failure_stage="trap cleanup"
				g_zxfer_failure_message=${g_zxfer_background_job_abort_failure_message:-Failed to tear down one or more supervised background jobs during exit.}
			fi
		fi
	fi
	l_cleanup_pid_status=0
	zxfer_kill_registered_cleanup_pids || l_cleanup_pid_status=$?
	if [ "$l_cleanup_pid_status" -ne 0 ]; then
		[ "$l_exit_status" -eq 0 ] && l_exit_status=$l_cleanup_pid_status
		if [ -z "${g_zxfer_failure_message:-}" ]; then
			g_zxfer_failure_class=runtime
			g_zxfer_failure_stage="trap cleanup"
			g_zxfer_failure_message=${g_zxfer_cleanup_pid_abort_failure_message:-Failed to tear down one or more validated cleanup helpers during exit.}
		fi
	fi

	if command -v zxfer_close_all_ssh_control_sockets >/dev/null 2>&1; then
		if zxfer_close_all_ssh_control_sockets; then
			:
		else
			l_close_status=$?
			if [ "$l_exit_status" -eq 0 ]; then
				l_exit_status=$l_close_status
				if [ -z "${g_zxfer_failure_message:-}" ]; then
					g_zxfer_failure_class=runtime
					g_zxfer_failure_stage="trap cleanup"
					g_zxfer_failure_message="Failed to close one or more ssh control sockets during exit."
				fi
			fi
		fi
	fi
	if command -v zxfer_release_registered_owned_locks >/dev/null 2>&1; then
		zxfer_release_registered_owned_locks || :
	fi

	zxfer_cleanup_registered_runtime_artifacts

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
			if command -v zxfer_remote_host_cache_cleanup_conflicts_with_path >/dev/null 2>&1 &&
				zxfer_remote_host_cache_cleanup_conflicts_with_path "$l_temp_file"; then
				continue
			fi
			if command -v zxfer_owned_lock_cleanup_conflicts_with_path >/dev/null 2>&1 &&
				zxfer_owned_lock_cleanup_conflicts_with_path "$l_temp_file"; then
				continue
			fi
			rm -rf "$l_temp_file"
		done
	fi
	if [ -n "${g_zxfer_property_cache_dir:-}" ] && [ -d "$g_zxfer_property_cache_dir" ]; then
		rm -rf "$g_zxfer_property_cache_dir"
	fi
	if [ -n "${g_zxfer_snapshot_index_dir:-}" ] && [ -d "$g_zxfer_snapshot_index_dir" ]; then
		rm -rf "$g_zxfer_snapshot_index_dir"
	fi
	if command -v zxfer_cleanup_remote_host_cache_roots >/dev/null 2>&1; then
		zxfer_cleanup_remote_host_cache_roots >/dev/null 2>&1 || :
	fi

	if [ "${g_services_need_relaunch:-0}" -eq 1 ]; then
		if [ "${g_services_relaunch_in_progress:-0}" -eq 1 ]; then
			zxfer_echoV "zxfer exiting with services still stopped after a failed zxfer_relaunch attempt."
		elif command -v zxfer_relaunch >/dev/null 2>&1; then
			zxfer_echoV "zxfer exiting early; restarting stopped services."
			zxfer_relaunch
		else
			zxfer_echoV "zxfer exiting with services still stopped; zxfer_relaunch() unavailable."
		fi
	fi

	zxfer_profile_add_elapsed_ms g_zxfer_profile_cleanup_ms "$l_cleanup_start_ms"
	zxfer_echoV "zxfer exiting with status $l_exit_status"
	zxfer_profile_emit_summary
	zxfer_emit_failure_report "$l_exit_status"

	# exit this script
	exit $l_exit_status
}

# Purpose: Register the runtime traps with the tracking state owned by this
# module.
# Usage: Called during runtime bootstrap, staging, and trap cleanup so cleanup
# and later lookups can find the live resource.
zxfer_register_runtime_traps() {
	# catch any signals to terminate the script
	# INT (Interrupt) 2 (Ctrl-C)
	# TERM (Terminate) 15 (kill)
	# HUP (Hangup) 1 (kill -HUP)
	# QUIT (Quit) 3 (Ctrl-\)
	# EXIT (Exit) 0 (exit)
	trap zxfer_trap_exit INT TERM HUP QUIT EXIT
}

# Purpose: Initialize the transfer command context before later helpers depend
# on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_transfer_command_context() {
	g_origin_cmd_compress_safe=$g_cmd_compress_safe
	g_origin_cmd_decompress_safe=$g_cmd_decompress_safe
	g_target_cmd_compress_safe=$g_cmd_compress_safe
	g_target_cmd_decompress_safe=$g_cmd_decompress_safe
}

# Purpose: Initialize the source execution context before later helpers depend
# on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_source_execution_context() {
	if [ "$g_option_O_origin_host" != "" ]; then
		if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
			g_source_operating_system=""
			g_origin_cmd_zfs=${g_origin_cmd_zfs:-$g_cmd_zfs}
			if [ "$g_option_z_compress" -eq 1 ] &&
				[ -z "${g_origin_cmd_compress_safe:-}" ]; then
				l_source_context_status=0
				g_origin_cmd_compress_safe=$(zxfer_quote_cli_tokens "$g_cmd_compress" "compression command") ||
					l_source_context_status=$?
				if [ "$l_source_context_status" -ne 0 ]; then
					zxfer_throw_error "$g_origin_cmd_compress_safe" "$l_source_context_status"
				fi
			fi
			zxfer_echoV "Dry run: skipping live remote source helper validation."
			return
		fi
		l_source_context_status=0
		g_source_operating_system=$(zxfer_get_os "$g_option_O_origin_host" source) ||
			l_source_context_status=$?
		if [ "$l_source_context_status" -ne 0 ]; then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "Failed to determine operating system on host $g_option_O_origin_host." "$l_source_context_status"
		fi
		l_source_context_status=0
		g_origin_cmd_zfs=$(zxfer_resolve_remote_required_tool "$g_option_O_origin_host" zfs "zfs" source) ||
			l_source_context_status=$?
		if [ "$l_source_context_status" -ne 0 ]; then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "$g_origin_cmd_zfs" "$l_source_context_status"
		fi
		if [ "$g_option_z_compress" -eq 1 ]; then
			l_source_context_status=0
			g_origin_cmd_compress_safe=$(zxfer_resolve_remote_cli_command_safe "$g_option_O_origin_host" "$g_cmd_compress" "compression command" source) ||
				l_source_context_status=$?
			if [ "$l_source_context_status" -ne 0 ]; then
				g_zxfer_failure_class=dependency
				zxfer_throw_error "$g_origin_cmd_compress_safe" "$l_source_context_status"
			fi
		fi
		return
	fi

	l_source_context_status=0
	g_source_operating_system=$(zxfer_get_os "") || l_source_context_status=$?
	if [ "$l_source_context_status" -ne 0 ]; then
		g_zxfer_failure_class=dependency
		zxfer_throw_error "Failed to determine the local operating system." "$l_source_context_status"
	fi
	g_origin_cmd_zfs=$g_cmd_zfs
}

# Purpose: Initialize the destination execution context before later helpers
# depend on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_destination_execution_context() {
	if [ "$g_option_T_target_host" != "" ]; then
		if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
			g_destination_operating_system=""
			g_target_cmd_zfs=${g_target_cmd_zfs:-$g_cmd_zfs}
			if [ "$g_option_z_compress" -eq 1 ] &&
				[ -z "${g_target_cmd_decompress_safe:-}" ]; then
				l_destination_context_status=0
				g_target_cmd_decompress_safe=$(zxfer_quote_cli_tokens "$g_cmd_decompress" "decompression command") ||
					l_destination_context_status=$?
				if [ "$l_destination_context_status" -ne 0 ]; then
					zxfer_throw_error "$g_target_cmd_decompress_safe" "$l_destination_context_status"
				fi
			fi
			zxfer_echoV "Dry run: skipping live remote destination helper validation."
			return
		fi
		l_destination_context_status=0
		g_destination_operating_system=$(zxfer_get_os "$g_option_T_target_host" destination) ||
			l_destination_context_status=$?
		if [ "$l_destination_context_status" -ne 0 ]; then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "Failed to determine operating system on host $g_option_T_target_host." "$l_destination_context_status"
		fi
		l_destination_context_status=0
		g_target_cmd_zfs=$(zxfer_resolve_remote_required_tool "$g_option_T_target_host" zfs "zfs" destination) ||
			l_destination_context_status=$?
		if [ "$l_destination_context_status" -ne 0 ]; then
			g_zxfer_failure_class=dependency
			zxfer_throw_error "$g_target_cmd_zfs" "$l_destination_context_status"
		fi
		if [ "$g_option_z_compress" -eq 1 ]; then
			l_destination_context_status=0
			g_target_cmd_decompress_safe=$(zxfer_resolve_remote_cli_command_safe "$g_option_T_target_host" "$g_cmd_decompress" "decompression command" destination) ||
				l_destination_context_status=$?
			if [ "$l_destination_context_status" -ne 0 ]; then
				g_zxfer_failure_class=dependency
				zxfer_throw_error "$g_target_cmd_decompress_safe" "$l_destination_context_status"
			fi
		fi
		return
	fi

	l_destination_context_status=0
	g_destination_operating_system=$(zxfer_get_os "") || l_destination_context_status=$?
	if [ "$l_destination_context_status" -ne 0 ]; then
		g_zxfer_failure_class=dependency
		zxfer_throw_error "Failed to determine the local operating system." "$l_destination_context_status"
	fi
	g_target_cmd_zfs=$g_cmd_zfs
}

# Purpose: Initialize the restore property helpers before later helpers depend
# on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_restore_property_helpers() {
	[ "$g_option_e_restore_property_mode" -eq 1 ] || return

	if [ "$g_option_O_origin_host" = "" ]; then
		zxfer_assign_required_tool g_cmd_cat cat "cat"
		return
	fi

	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		[ -n "${g_cmd_cat:-}" ] || g_cmd_cat="cat"
		zxfer_echoV "Dry run: skipping live remote backup-restore helper validation."
		return
	fi

	l_restore_property_status=0
	g_cmd_cat=$(zxfer_resolve_remote_required_tool "$g_option_O_origin_host" cat "cat" source) ||
		l_restore_property_status=$?
	if [ "$l_restore_property_status" -ne 0 ]; then
		g_zxfer_failure_class=dependency
		zxfer_throw_error "$g_cmd_cat" "$l_restore_property_status"
	fi
}

# Purpose: Initialize the local awk compatibility before later helpers depend
# on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_local_awk_compatibility() {
	l_awk_compatibility_status=0
	l_home_operating_system=$(zxfer_get_os "") || l_awk_compatibility_status=$?
	if [ "$l_awk_compatibility_status" -ne 0 ]; then
		g_zxfer_failure_class=dependency
		zxfer_throw_error "Failed to determine the local operating system." "$l_awk_compatibility_status"
	fi
	if [ "$l_home_operating_system" != "SunOS" ]; then
		return
	fi

	l_gawk_path=$(PATH=$g_zxfer_dependency_path command -v gawk 2>/dev/null || :)
	if [ "$l_gawk_path" != "" ]; then
		g_cmd_awk=$l_gawk_path
	fi
}

# Purpose: Initialize the variables before later helpers depend on it.
# Usage: Called during runtime bootstrap, staging, and trap cleanup during
# bootstrap so downstream code sees consistent defaults and runtime state.
zxfer_init_variables() {
	zxfer_init_transfer_command_context
	zxfer_init_source_execution_context
	zxfer_init_destination_execution_context
	zxfer_refresh_remote_zfs_commands
	zxfer_init_restore_property_helpers
	zxfer_init_local_awk_compatibility
}
