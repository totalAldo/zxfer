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
# shellcheck shell=sh

# Purpose: Return a portable process-start token for one PID without trusting
# the PID alone across a process-table discovery boundary.
# Usage: Signal-time descendant snapshots call this only during abort; normal
# command execution performs no process-table probes.
zxfer_cleanup_child_wrapper_get_process_start_token() {
	l_cleanup_wrapper_token_pid=$1
	l_cleanup_wrapper_token_selector=${2:-lstart}
	case "$l_cleanup_wrapper_token_pid" in
	'' | *[!0-9]*) return 1 ;;
	esac
	case "$l_cleanup_wrapper_token_selector" in
	lstart | stime) ;;
	*) return 1 ;;
	esac

	l_cleanup_wrapper_token_output=$(LC_ALL=C ps \
		-o "$l_cleanup_wrapper_token_selector=" -p \
		"$l_cleanup_wrapper_token_pid" 2>/dev/null) ||
		l_cleanup_wrapper_token_output=""
	if [ -z "$l_cleanup_wrapper_token_output" ]; then
		l_cleanup_wrapper_token_output=$(LC_ALL=C ps \
			-o "$l_cleanup_wrapper_token_selector" -p \
			"$l_cleanup_wrapper_token_pid" 2>/dev/null) ||
			l_cleanup_wrapper_token_output=""
		l_cleanup_wrapper_token_header_output=$l_cleanup_wrapper_token_output
		l_cleanup_wrapper_token_output=""
		l_cleanup_wrapper_token_line_number=0
		while IFS= read -r l_cleanup_wrapper_token_line || [ -n "$l_cleanup_wrapper_token_line" ]; do
			l_cleanup_wrapper_token_line_number=$((l_cleanup_wrapper_token_line_number + 1))
			[ "$l_cleanup_wrapper_token_line_number" -eq 2 ] || continue
			l_cleanup_wrapper_token_output=$l_cleanup_wrapper_token_line
			break
		done <<-EOF
			$l_cleanup_wrapper_token_header_output
		EOF
	fi
	case $- in
	*f*) l_cleanup_wrapper_token_restore_glob=0 ;;
	*)
		l_cleanup_wrapper_token_restore_glob=1
		set -f
		;;
	esac
	if [ "${IFS+set}" = set ]; then
		l_cleanup_wrapper_token_saved_ifs_set=1
		l_cleanup_wrapper_token_saved_ifs=$IFS
	else
		l_cleanup_wrapper_token_saved_ifs_set=0
		l_cleanup_wrapper_token_saved_ifs=""
	fi
	unset IFS
	# shellcheck disable=SC2086
	set -- $l_cleanup_wrapper_token_output
	l_cleanup_wrapper_token_argc=$#
	l_cleanup_wrapper_token_normalized=$*
	if [ "$l_cleanup_wrapper_token_saved_ifs_set" -eq 1 ]; then
		IFS=$l_cleanup_wrapper_token_saved_ifs
	else
		unset IFS
	fi
	[ "$l_cleanup_wrapper_token_restore_glob" -eq 0 ] || set +f
	[ "$l_cleanup_wrapper_token_argc" -gt 0 ] || return 1
	printf '%s:%s\n' \
		"$l_cleanup_wrapper_token_selector" "$l_cleanup_wrapper_token_normalized"
}

# Capture descendant PID/start-token pairs in one process-table snapshot. The
# token is revalidated immediately before each signal so a recycled PID is
# never treated as part of the wrapper's process tree.
zxfer_cleanup_child_wrapper_list_descendants() {
	l_cleanup_wrapper_snapshot_roots=${1:-$$}
	l_cleanup_wrapper_snapshot_status=0
	# -A is required: without it ps only lists same-terminal processes, so a
	# wrapper running without a controlling terminal (cron, CI, supervised
	# background jobs) would miss its own descendants and leak them on TERM.
	l_cleanup_wrapper_snapshot_selector=lstart
	if l_cleanup_wrapper_snapshot=$(LC_ALL=C ps -A -o pid= -o ppid= -o lstart= 2>/dev/null); then
		:
	elif l_cleanup_wrapper_snapshot=$(LC_ALL=C ps -A -o pid -o ppid -o lstart 2>/dev/null); then
		:
	else
		l_cleanup_wrapper_snapshot_selector=stime
		if l_cleanup_wrapper_snapshot=$(LC_ALL=C ps -A -o pid= -o ppid= -o stime= 2>/dev/null); then
			:
		else
			l_cleanup_wrapper_snapshot=$(LC_ALL=C ps -A -o pid -o ppid -o stime 2>/dev/null) ||
				l_cleanup_wrapper_snapshot_status=$?
		fi
	fi
	[ "$l_cleanup_wrapper_snapshot_status" -eq 0 ] || return "$l_cleanup_wrapper_snapshot_status"

	l_cleanup_wrapper_descendants_status=0
	l_cleanup_wrapper_descendants=$(printf '%s\n' "$l_cleanup_wrapper_snapshot" |
		awk -v roots="$l_cleanup_wrapper_snapshot_roots" -v selector="$l_cleanup_wrapper_snapshot_selector" '
	{
		pid = $1
		ppid = $2
		if (pid ~ /^[0-9]+$/ && ppid ~ /^[0-9]+$/) {
			parent[pid] = ppid
			seen[pid] = 1
			token = ""
			for (field = 3; field <= NF; field++)
				token = token (token == "" ? "" : " ") $field
			start_token[pid] = selector ":" token
		}
	}
	END {
		root_count = split(roots, root_pid, " ")
		primary_root = root_pid[1]
		if (!(primary_root in seen)) exit 1
		for (root_index = 1; root_index <= root_count; root_index++)
			if (root_pid[root_index] in seen)
				target[root_pid[root_index]] = 1
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
			if (pid != primary_root) {
				if (start_token[pid] == selector ":") exit 1
				print pid "\t" start_token[pid]
			}
		}
	}') || l_cleanup_wrapper_descendants_status=$?
	[ "$l_cleanup_wrapper_descendants_status" -eq 0 ] ||
		return "$l_cleanup_wrapper_descendants_status"

	printf '%s\n' "$l_cleanup_wrapper_descendants" | LC_ALL=C sort -nr
}

# Return roots that still match retained identity records. These validated
# roots let the post-TERM refresh find children of cooperative helpers that
# remain alive during the grace window. POSIX ancestry snapshots cannot recover
# an unknown child after its parent exits and the kernel reparents it.
zxfer_cleanup_child_wrapper_build_validated_descendant_roots() {
	l_cleanup_wrapper_root_records=$1
	l_cleanup_wrapper_validated_roots=$$
	l_cleanup_wrapper_validated_status=0

	while IFS='	' read -r l_cleanup_wrapper_root_pid l_cleanup_wrapper_root_token || [ -n "${l_cleanup_wrapper_root_pid}${l_cleanup_wrapper_root_token}" ]; do
		[ -n "$l_cleanup_wrapper_root_pid" ] || continue
		l_cleanup_wrapper_root_selector=${l_cleanup_wrapper_root_token%%:*}
		if l_cleanup_wrapper_current_token=$(
			zxfer_cleanup_child_wrapper_get_process_start_token \
				"$l_cleanup_wrapper_root_pid" \
				"$l_cleanup_wrapper_root_selector" 2>/dev/null
		); then
			if [ "$l_cleanup_wrapper_current_token" = "$l_cleanup_wrapper_root_token" ]; then
				l_cleanup_wrapper_validated_roots="$l_cleanup_wrapper_validated_roots $l_cleanup_wrapper_root_pid"
			else
				l_cleanup_wrapper_validated_status=1
			fi
		elif kill -s 0 "$l_cleanup_wrapper_root_pid" 2>/dev/null; then
			l_cleanup_wrapper_validated_status=1
		fi
	done <<-EOF
		$l_cleanup_wrapper_root_records
	EOF

	printf '%s\n' "$l_cleanup_wrapper_validated_roots"
	return "$l_cleanup_wrapper_validated_status"
}

# Signal one retained descendant identity snapshot. A process that exited is a
# success; a still-live PID whose token cannot be matched is never signalled.
zxfer_cleanup_child_wrapper_signal_descendant_records() {
	l_cleanup_wrapper_descendants=$1
	l_cleanup_wrapper_descendants_signal=${2:-TERM}
	l_cleanup_wrapper_descendants_status=0

	while IFS='	' read -r l_cleanup_wrapper_pid l_cleanup_wrapper_start_token || [ -n "${l_cleanup_wrapper_pid}${l_cleanup_wrapper_start_token}" ]; do
		[ -n "$l_cleanup_wrapper_pid" ] || continue
		l_cleanup_wrapper_expected_selector=${l_cleanup_wrapper_start_token%%:*}
		l_cleanup_wrapper_current_token=$(
			zxfer_cleanup_child_wrapper_get_process_start_token \
				"$l_cleanup_wrapper_pid" \
				"$l_cleanup_wrapper_expected_selector" 2>/dev/null
		) || {
			kill -s 0 "$l_cleanup_wrapper_pid" 2>/dev/null &&
				l_cleanup_wrapper_descendants_status=1
			continue
		}
		if [ "$l_cleanup_wrapper_current_token" != "$l_cleanup_wrapper_start_token" ]; then
			l_cleanup_wrapper_descendants_status=1
			continue
		fi
		if ! kill -s "$l_cleanup_wrapper_descendants_signal" \
			"$l_cleanup_wrapper_pid" 2>/dev/null; then
			kill -s 0 "$l_cleanup_wrapper_pid" 2>/dev/null &&
				l_cleanup_wrapper_descendants_status=1
		fi
	done <<-EOF
		$l_cleanup_wrapper_descendants
	EOF
	return "$l_cleanup_wrapper_descendants_status"
}

zxfer_cleanup_child_wrapper_abort_descendants() {
	l_cleanup_wrapper_abort_signal=${1:-TERM}
	l_cleanup_wrapper_descendants_status=0
	l_cleanup_wrapper_descendants=$(zxfer_cleanup_child_wrapper_list_descendants) ||
		l_cleanup_wrapper_descendants_status=$?
	[ "$l_cleanup_wrapper_descendants_status" -eq 0 ] || return "$l_cleanup_wrapper_descendants_status"

	zxfer_cleanup_child_wrapper_signal_descendant_records \
		"$l_cleanup_wrapper_descendants" "$l_cleanup_wrapper_abort_signal"
}

zxfer_cleanup_child_wrapper_abort_grace_wait() {
	sleep 1
}

# Stop the wrapper-owned direct child before refreshed ancestry snapshots.
# A failed STOP is an error only while that exact un-waited child remains live.
zxfer_cleanup_child_wrapper_stop_direct_child() {
	l_cleanup_wrapper_stop_pid=${l_cleanup_wrapper_child_pid:-}
	[ -n "$l_cleanup_wrapper_stop_pid" ] || return 0
	kill -s 0 "$l_cleanup_wrapper_stop_pid" 2>/dev/null || return 0
	kill -s STOP "$l_cleanup_wrapper_stop_pid" 2>/dev/null && return 0
	kill -s 0 "$l_cleanup_wrapper_stop_pid" 2>/dev/null || return 0
	return 1
}

# Extend one retained identity set with a fresh best-effort ancestry snapshot,
# then STOP every newly observed cooperative helper before the next refresh.
# The combined records are published in the owner-prefixed result global even
# when one identity cannot be proven, so the caller can still tear down every
# independently validated process. This helper must run in the wrapper shell:
# a command-substitution subshell would appear in its own ancestry snapshot and
# could STOP itself before publishing the result.
zxfer_cleanup_child_wrapper_extend_stopped_descendant_records() {
	l_cleanup_wrapper_extend_records=$1
	l_cleanup_wrapper_extend_status=0
	g_zxfer_cleanup_wrapper_extended_records=$l_cleanup_wrapper_extend_records
	l_cleanup_wrapper_extend_roots=$(zxfer_cleanup_child_wrapper_build_validated_descendant_roots \
		"$l_cleanup_wrapper_extend_records") || l_cleanup_wrapper_extend_status=$?
	l_cleanup_wrapper_extend_new_records=""
	l_cleanup_wrapper_extend_list_status=0
	l_cleanup_wrapper_extend_new_records=$(zxfer_cleanup_child_wrapper_list_descendants \
		"$l_cleanup_wrapper_extend_roots") || l_cleanup_wrapper_extend_list_status=$?
	if [ "$l_cleanup_wrapper_extend_list_status" -eq 0 ]; then
		zxfer_cleanup_child_wrapper_signal_descendant_records \
			"$l_cleanup_wrapper_extend_new_records" STOP >/dev/null 2>&1 || {
			l_cleanup_wrapper_extend_stop_status=$?
			[ "$l_cleanup_wrapper_extend_status" -ne 0 ] ||
				l_cleanup_wrapper_extend_status=$l_cleanup_wrapper_extend_stop_status
		}
	elif [ "$l_cleanup_wrapper_extend_status" -eq 0 ]; then
		l_cleanup_wrapper_extend_status=$l_cleanup_wrapper_extend_list_status
	fi
	if [ -n "$l_cleanup_wrapper_extend_new_records" ]; then
		if [ -n "$g_zxfer_cleanup_wrapper_extended_records" ]; then
			g_zxfer_cleanup_wrapper_extended_records=$g_zxfer_cleanup_wrapper_extended_records"
$l_cleanup_wrapper_extend_new_records"
		else
			g_zxfer_cleanup_wrapper_extended_records=$l_cleanup_wrapper_extend_new_records
		fi
	fi
	return "$l_cleanup_wrapper_extend_status"
}

# KILL and reap the wrapper-owned direct child. Never wait after a failed KILL
# while the child is still live, because that would make trap cleanup unbounded.
zxfer_cleanup_child_wrapper_kill_and_wait_direct_child() {
	l_cleanup_wrapper_kill_pid=${l_cleanup_wrapper_child_pid:-}
	[ -n "$l_cleanup_wrapper_kill_pid" ] || return 0
	if kill -s 0 "$l_cleanup_wrapper_kill_pid" 2>/dev/null; then
		if ! kill -s KILL "$l_cleanup_wrapper_kill_pid" 2>/dev/null; then
			kill -s 0 "$l_cleanup_wrapper_kill_pid" 2>/dev/null && return 1
		fi
	fi
	wait "$l_cleanup_wrapper_kill_pid" 2>/dev/null || :
	return 0
}

# Bounded fallback teardown for hosts where no isolated process group is
# available. The direct `$!` child is always stopped, KILLed, and reaped.
# Descendant refresh is intentionally best-effort: POSIX process snapshots
# cannot contain an arbitrary helper that forks and exits during TERM grace.
zxfer_cleanup_child_wrapper_on_signal() {
	l_cleanup_wrapper_snapshot_status=0
	l_cleanup_wrapper_signal_records=$(zxfer_cleanup_child_wrapper_list_descendants) ||
		l_cleanup_wrapper_snapshot_status=$?
	l_cleanup_wrapper_term_status=$l_cleanup_wrapper_snapshot_status
	if [ "$l_cleanup_wrapper_snapshot_status" -eq 0 ]; then
		zxfer_cleanup_child_wrapper_signal_descendant_records \
			"$l_cleanup_wrapper_signal_records" TERM >/dev/null 2>&1 ||
			l_cleanup_wrapper_term_status=$?
	fi
	if [ "$l_cleanup_wrapper_term_status" -ne 0 ] &&
		[ -n "${l_cleanup_wrapper_child_pid:-}" ]; then
		kill -s TERM "$l_cleanup_wrapper_child_pid" 2>/dev/null || :
	fi

	zxfer_cleanup_child_wrapper_abort_grace_wait
	l_cleanup_wrapper_teardown_status=$l_cleanup_wrapper_snapshot_status
	zxfer_cleanup_child_wrapper_stop_direct_child ||
		l_cleanup_wrapper_teardown_status=$?

	l_cleanup_wrapper_kill_records=$l_cleanup_wrapper_signal_records
	l_cleanup_wrapper_refresh_status=0
	zxfer_cleanup_child_wrapper_extend_stopped_descendant_records \
		"$l_cleanup_wrapper_kill_records" || l_cleanup_wrapper_refresh_status=$?
	l_cleanup_wrapper_kill_records=$g_zxfer_cleanup_wrapper_extended_records
	[ "$l_cleanup_wrapper_teardown_status" -ne 0 ] ||
		l_cleanup_wrapper_teardown_status=$l_cleanup_wrapper_refresh_status

	l_cleanup_wrapper_refresh_status=0
	zxfer_cleanup_child_wrapper_extend_stopped_descendant_records \
		"$l_cleanup_wrapper_kill_records" || l_cleanup_wrapper_refresh_status=$?
	l_cleanup_wrapper_kill_records=$g_zxfer_cleanup_wrapper_extended_records
	[ "$l_cleanup_wrapper_teardown_status" -ne 0 ] ||
		l_cleanup_wrapper_teardown_status=$l_cleanup_wrapper_refresh_status

	l_cleanup_wrapper_kill_status=0
	zxfer_cleanup_child_wrapper_signal_descendant_records \
		"$l_cleanup_wrapper_kill_records" KILL >/dev/null 2>&1 ||
		l_cleanup_wrapper_kill_status=$?
	[ "$l_cleanup_wrapper_teardown_status" -ne 0 ] ||
		l_cleanup_wrapper_teardown_status=$l_cleanup_wrapper_kill_status
	zxfer_cleanup_child_wrapper_kill_and_wait_direct_child ||
		l_cleanup_wrapper_teardown_status=$?
	[ "$l_cleanup_wrapper_teardown_status" -eq 0 ] || exit 125
	exit 143
}

zxfer_cleanup_child_wrapper_main() {
	l_cleanup_wrapper_exec_cmd=$1
	[ -n "$l_cleanup_wrapper_exec_cmd" ] || return 1
	trap 'zxfer_cleanup_child_wrapper_on_signal' TERM INT HUP
	l_cleanup_wrapper_status=0
	exec 3<&0 || l_cleanup_wrapper_status=$?
	[ "$l_cleanup_wrapper_status" -eq 0 ] || return "$l_cleanup_wrapper_status"

	# Preserve the wrapper's stdin for background children. Some /bin/sh
	# implementations reattach asynchronous jobs to /dev/null unless stdin is
	# duplicated onto a dedicated descriptor before the background launch.
	/bin/sh -c "$l_cleanup_wrapper_exec_cmd" <&3 &
	l_cleanup_wrapper_child_pid=$!
	l_cleanup_wrapper_status=0
	wait "$l_cleanup_wrapper_child_pid" || l_cleanup_wrapper_status=$?
	exec 3<&-
	return "$l_cleanup_wrapper_status"
}

if [ "${ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY:-0}" != "1" ]; then
	zxfer_cleanup_child_wrapper_main "$@"
	exit $?
fi
