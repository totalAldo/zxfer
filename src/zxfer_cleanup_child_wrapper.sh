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

# Cleanup child wrapper: runs one rendered command as a background child and,
# on TERM, INT or HUP, tears down that child's descendants, each identified by
# pid plus process start token. Sourced with
# ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY=1 it only defines functions, and
# zxfer_exec.sh reuses the descendant teardown.

# Purpose: Print PID's start time as "SELECTOR:TIME" (whitespace squeezed), the
# token that tells a process from a later one reusing its pid. This is zxfer's
# only per-PID start-token parser.
# Usage: zxfer_cleanup_child_wrapper_get_process_start_token PID
# [lstart|stime] (default lstart); returns 1 for a bad argument or when ps has
# no record. Caller IFS and noglob state are preserved.
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
		# Some ps builds (FreeBSD) reject the header-less form for this
		# field; keep only the line after the header.
		l_cleanup_wrapper_token_output=$(LC_ALL=C ps \
			-o "$l_cleanup_wrapper_token_selector" -p \
			"$l_cleanup_wrapper_token_pid" 2>/dev/null) ||
			l_cleanup_wrapper_token_output=""
		l_cleanup_wrapper_token_newline='
'
		case $l_cleanup_wrapper_token_output in
		*"$l_cleanup_wrapper_token_newline"*)
			l_cleanup_wrapper_token_output=${l_cleanup_wrapper_token_output#*"$l_cleanup_wrapper_token_newline"}
			l_cleanup_wrapper_token_output=${l_cleanup_wrapper_token_output%%"$l_cleanup_wrapper_token_newline"*}
			;;
		*)
			l_cleanup_wrapper_token_output=""
			;;
		esac
	fi
	# Squeeze whitespace by field splitting with the default IFS and globbing
	# off, restoring both afterwards.
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

# Purpose: Print "PID<TAB>TOKEN" for every descendant of ROOTS from one
# process-table snapshot, highest pid first.
# Usage: zxfer_cleanup_child_wrapper_list_descendants [ROOTS] (default $$; the
# first root must be listed). Tokens are revalidated just before each signal,
# so a recycled pid is never treated as part of the tree.
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

# Purpose: Tell whether PID still runs: it takes signals and is not a zombie.
# Usage: zxfer_cleanup_child_wrapper_pid_is_running PID, for a pid whose
# recorded start token no longer matches or cannot be read. A zombie (a child
# that exited while its parent was stopped) is gone; illumos may report no
# start time for one, which changes its token.
zxfer_cleanup_child_wrapper_pid_is_running() {
	kill -s 0 "$1" 2>/dev/null || return 1
	# BSD and Linux ps call the state column stat; illumos calls it s.
	l_cleanup_wrapper_state=$(LC_ALL=C ps -o stat= -p "$1" 2>/dev/null) ||
		l_cleanup_wrapper_state=$(LC_ALL=C ps -o s= -p "$1" 2>/dev/null) ||
		return 0
	case $l_cleanup_wrapper_state in
	*Z*) return 1 ;;
	esac
	return 0
}

# Purpose: Print "$$" plus every recorded pid whose start token still matches,
# the roots that let the post-TERM refresh find children of helpers still
# alive in the grace window.
# Usage: zxfer_cleanup_child_wrapper_build_validated_descendant_roots RECORDS;
# returns 1 when a running pid has another token or none. A child whose
# parent already exited was reparented and cannot be found this way.
zxfer_cleanup_child_wrapper_build_validated_descendant_roots() {
	l_cleanup_wrapper_root_records=$1
	l_cleanup_wrapper_validated_roots=$$
	l_cleanup_wrapper_validated_status=0

	while IFS='	' read -r l_cleanup_wrapper_root_pid l_cleanup_wrapper_root_token || [ -n "${l_cleanup_wrapper_root_pid}${l_cleanup_wrapper_root_token}" ]; do
		[ -n "$l_cleanup_wrapper_root_pid" ] || continue
		l_cleanup_wrapper_root_selector=${l_cleanup_wrapper_root_token%%:*}
		l_cleanup_wrapper_current_token=$(
			zxfer_cleanup_child_wrapper_get_process_start_token \
				"$l_cleanup_wrapper_root_pid" \
				"$l_cleanup_wrapper_root_selector" 2>/dev/null
		) || l_cleanup_wrapper_current_token=""
		if [ -n "$l_cleanup_wrapper_current_token" ] &&
			[ "$l_cleanup_wrapper_current_token" = "$l_cleanup_wrapper_root_token" ]; then
			l_cleanup_wrapper_validated_roots="$l_cleanup_wrapper_validated_roots $l_cleanup_wrapper_root_pid"
		elif zxfer_cleanup_child_wrapper_pid_is_running "$l_cleanup_wrapper_root_pid"; then
			l_cleanup_wrapper_validated_status=1
		fi
	done <<-EOF
		$l_cleanup_wrapper_root_records
	EOF

	printf '%s\n' "$l_cleanup_wrapper_validated_roots"
	return "$l_cleanup_wrapper_validated_status"
}

# Purpose: Send SIGNAL to each recorded pid whose start token still matches.
# Usage: zxfer_cleanup_child_wrapper_signal_descendant_records RECORDS
# [SIGNAL] (default TERM); an exited process counts as success, and a pid
# whose token cannot be matched is never signalled and makes it return 1
# while it still runs.
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
		) || l_cleanup_wrapper_current_token=""
		if [ -z "$l_cleanup_wrapper_current_token" ] ||
			[ "$l_cleanup_wrapper_current_token" != "$l_cleanup_wrapper_start_token" ]; then
			zxfer_cleanup_child_wrapper_pid_is_running "$l_cleanup_wrapper_pid" &&
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

# Purpose: Give TERMed descendants one second to exit.
# Usage: zxfer_cleanup_child_wrapper_abort_grace_wait (tests replace it).
zxfer_cleanup_child_wrapper_abort_grace_wait() {
	sleep 1
}

# Purpose: Add a fresh best-effort ancestry snapshot to RECORDS and STOP every
# newly seen helper before the next refresh.
# Usage: zxfer_cleanup_child_wrapper_extend_stopped_descendant_records
# RECORDS; sets g_zxfer_cleanup_wrapper_extended_records even when an identity
# cannot be proven, so the caller still tears down every validated process.
# Call it in the wrapper shell: a $(...) subshell would appear in its own
# snapshot and could STOP itself.
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

# Purpose: KILL and reap the wrapper's direct child.
# Usage: zxfer_cleanup_child_wrapper_kill_and_wait_direct_child; when KILL
# fails on a live child it returns 1 without waiting, so trap cleanup stays
# bounded.
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

# Purpose: Bounded signal teardown for hosts without an isolated process
# group: TERM the descendants, wait one grace second, then STOP, refresh twice
# and KILL them; the direct child is always stopped, KILLed and reaped.
# Usage: the TERM/INT/HUP trap of zxfer_cleanup_child_wrapper_main; exits 143,
# or 125 when a teardown step cannot be verified. Refresh is best effort: a
# helper that forks and exits during the grace window can escape a snapshot.
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
	# STOP the direct child before the refreshed ancestry snapshots. A failed
	# STOP counts only while that un-waited child is still alive.
	if [ -n "${l_cleanup_wrapper_child_pid:-}" ] &&
		kill -s 0 "$l_cleanup_wrapper_child_pid" 2>/dev/null &&
		! kill -s STOP "$l_cleanup_wrapper_child_pid" 2>/dev/null &&
		kill -s 0 "$l_cleanup_wrapper_child_pid" 2>/dev/null; then
		l_cleanup_wrapper_teardown_status=1
	fi

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

# Purpose: Run CMD under /bin/sh -c as a background child, with the signal
# teardown armed, and wait for it.
# Usage: zxfer_cleanup_child_wrapper_main CMD [ARG...], where the ARGs become
# CMD's $1...; returns CMD's status, or 1 when CMD is missing or empty.
zxfer_cleanup_child_wrapper_main() {
	[ -n "${1:-}" ] || return 1
	l_cleanup_wrapper_exec_cmd=$1
	shift
	trap 'zxfer_cleanup_child_wrapper_on_signal' TERM INT HUP
	l_cleanup_wrapper_status=0
	exec 3<&0 || l_cleanup_wrapper_status=$?
	[ "$l_cleanup_wrapper_status" -eq 0 ] || return "$l_cleanup_wrapper_status"

	# Preserve the wrapper's stdin for background children. Some /bin/sh
	# implementations reattach asynchronous jobs to /dev/null unless stdin is
	# duplicated onto a dedicated descriptor before the background launch.
	/bin/sh -c "$l_cleanup_wrapper_exec_cmd" zxfer-job "$@" <&3 &
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
