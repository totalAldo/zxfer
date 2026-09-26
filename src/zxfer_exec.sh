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
# COMMAND EXECUTION HELPERS
################################################################################

# Module contract:
# owns globals: generic command execution scratch, g_last_background_pid,
# g_zxfer_background_shell_scope, and the once-per-process background shell
# spawn mode g_zxfer_background_shell_spawn_mode.
# reads globals: dry-run/reporting state plus runtime cleanup helpers.
# mutates caches: cleanup PID tracking through shared helpers.
# returns via stdout: command output only.
#
# Background shells run `/bin/sh -c CMD` in their own process group whenever
# the host can provide one: setsid(1) when it works, otherwise verified
# non-interactive shell job control (bash) on a process without a
# controlling terminal. One group signal then stops the whole pipeline.
# Without either, the cleanup child wrapper retains descendant ownership;
# its slower process-table checks run only during abnormal teardown.
# Job shells run /bin/sh, the launcher's #! interpreter, so a narrow
# ZXFER_SECURE_PATH never has to list sh.
# Every process-group probe and signal goes through
# zxfer_signal_process_group, which owns the one `kill` form all supported
# shells read; the job-shell probes below use the same form literally.

# Purpose: Forget the probed background spawn mode.
# Usage: Called before traps become active so an exported mode cannot select
# a spawn path the host never verified.
zxfer_reset_background_shell_spawn_mode() {
	g_zxfer_background_shell_spawn_mode=""
	g_zxfer_background_shell_scope=""
}

# Purpose: Run one command string built by zxfer's hardened renderers; this
# is zxfer's single eval site.
# Usage: zxfer_execute_rendered_shell_command CMD; honors -n, records CMD for
# failure reports, echoes it under -v, and throws when it fails.
zxfer_execute_rendered_shell_command() {
	l_cmd=$1
	zxfer_record_last_command_string "$l_cmd"

	if [ "$g_option_n_dryrun" -eq 1 ]; then
		zxfer_echov "Dry run: $l_cmd"
		return 0
	fi

	zxfer_echov "$l_cmd"
	eval "$l_cmd" || zxfer_throw_error "Error when executing command."
}

# Purpose: Decide once per process how background shells are isolated.
# Usage: Called lazily by the first spawn; suites may pre-set
# $g_zxfer_background_shell_spawn_mode to setsid, monitor, or wrapper.
zxfer_init_background_shell_spawn_mode() {
	[ -z "${g_zxfer_background_shell_spawn_mode:-}" ] || return 0
	g_zxfer_background_shell_spawn_mode=wrapper
	# Verify both group isolation and PID preservation. Some setsid variants
	# fork; their launcher PID is not the group ID the parent must signal.
	if command -v setsid >/dev/null 2>&1; then
		l_spawn_mode_probe=$(
			setsid /bin/sh -c 'kill -0 "-$$" && printf "%s\n" "$$"' </dev/null 2>/dev/null &
			l_spawn_mode_probe_pid=$!
			wait "$l_spawn_mode_probe_pid" 2>/dev/null || exit 1
			printf '%s\n' "$l_spawn_mode_probe_pid"
		) || l_spawn_mode_probe=""
		if [ "${l_spawn_mode_probe#*
}" != "$l_spawn_mode_probe" ] &&
			[ "${l_spawn_mode_probe%%
*}" = "${l_spawn_mode_probe#*
}" ]; then
			g_zxfer_background_shell_spawn_mode=setsid
			return 0
		fi
	fi
	# Shell job control also gives every background job its own group, but
	# some shells initialize it by taking over the controlling terminal and
	# stop zxfer when it is not that terminal's foreground job, so it is
	# only tried when there is no controlling terminal to take. The probe
	# runs in a subshell because the mode must hold wherever zxfer spawns,
	# and FreeBSD sh, dash and ksh93 (illumos /bin/sh) ignore set -m there.
	if (
		exec 2>/dev/null
		true </dev/tty && exit 1
		set -m || exit 1
		/bin/sh -c 'kill -0 "-$$"' </dev/null >/dev/null &
		wait "$!"
	); then
		g_zxfer_background_shell_spawn_mode=monitor
	fi
	return 0
}

# Purpose: Start `/bin/sh -c CMD` in the background with the selected
# isolation.
# Usage: zxfer_spawn_background_shell CMD [STDOUT_FILE] [STDERR_FILE] [ARG ...]
# Empty file arguments inherit zxfer's descriptors; stdin is always
# /dev/null; ARGs become the job shell's positional parameters.
# Side effects: Publishes the child pid in $g_last_background_pid and its
# signal scope (pgid or wrapper) in $g_zxfer_background_shell_scope.
zxfer_spawn_background_shell() {
	l_spawn_shell_cmd=$1
	g_last_background_pid=""
	l_spawn_shell_stdout=${2:-}
	l_spawn_shell_stderr=${3:-}
	if [ "$#" -ge 3 ]; then
		shift 3
	else
		shift "$#"
	fi

	zxfer_init_background_shell_spawn_mode
	g_zxfer_background_shell_scope=pgid
	case $g_zxfer_background_shell_spawn_mode in
	setsid)
		set -- setsid /bin/sh -c "$l_spawn_shell_cmd" zxfer-job "$@"
		;;
	monitor)
		set -- /bin/sh -c "$l_spawn_shell_cmd" zxfer-job "$@"
		set -m
		;;
	*)
		l_spawn_shell_wrapper=$(zxfer_get_cleanup_child_wrapper_script_path) || return 1
		set -- /bin/sh "$l_spawn_shell_wrapper" "$l_spawn_shell_cmd" "$@"
		g_zxfer_background_shell_scope=wrapper
		;;
	esac
	if [ -n "$l_spawn_shell_stdout" ] && [ -n "$l_spawn_shell_stderr" ]; then
		"$@" </dev/null >"$l_spawn_shell_stdout" 2>"$l_spawn_shell_stderr" &
	elif [ -n "$l_spawn_shell_stdout" ]; then
		"$@" </dev/null >"$l_spawn_shell_stdout" &
	elif [ -n "$l_spawn_shell_stderr" ]; then
		"$@" </dev/null 2>"$l_spawn_shell_stderr" &
	else
		"$@" </dev/null &
	fi
	g_last_background_pid=$!
	[ "$g_zxfer_background_shell_spawn_mode" != monitor ] || set +m
}

# Purpose: Stop a fallback wrapper and its validated descendants.
# Usage: Cold KILL escalation only; keep process-table parsing out of normal
# execution. The wrapper is stopped before snapshotting so it cannot spawn
# more children between discovery and signalling.
zxfer_kill_background_wrapper() (
	l_kill_wrapper_pid=$1
	ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY=1
	l_kill_wrapper_script=$(zxfer_get_cleanup_child_wrapper_script_path) || return 1
	# shellcheck source=src/zxfer_cleanup_child_wrapper.sh
	. "$l_kill_wrapper_script"
	kill -s STOP "$l_kill_wrapper_pid" 2>/dev/null || {
		! kill -s 0 "$l_kill_wrapper_pid" 2>/dev/null
		return "$?"
	}
	l_kill_wrapper_status=0
	if l_kill_wrapper_descendants=$(zxfer_cleanup_child_wrapper_list_descendants "$l_kill_wrapper_pid"); then
		zxfer_cleanup_child_wrapper_signal_descendant_records \
			"$l_kill_wrapper_descendants" KILL || l_kill_wrapper_status=$?
	else
		l_kill_wrapper_status=$?
	fi
	kill -s KILL "$l_kill_wrapper_pid" 2>/dev/null || {
		kill -s 0 "$l_kill_wrapper_pid" 2>/dev/null && l_kill_wrapper_status=1
	}
	return "$l_kill_wrapper_status"
)

# Purpose: Send one signal to a whole process group, or probe it with 0.
# Usage: zxfer_signal_process_group SIGNAL PGID; SIGNAL is a name such as
# TERM or KILL, or 0. Returns kill's status. This is the signal-first
# `kill -SIG -PGID` form because it is the one every supported shell reads:
# dash and bash 3.2 reject `kill -s SIG -PGID`, and BusyBox ash takes the
# `--` of `kill -s SIG -- -PGID` for a PID, signalling the group but
# exiting 1.
zxfer_signal_process_group() {
	kill "-$1" "-$2" 2>/dev/null
}

# Purpose: Signal one spawned background shell through its recorded scope.
# Usage: zxfer_signal_background_shell PID SCOPE SIGNAL; a pgid scope
# signals the whole process group. Returns non-zero when a live target cannot
# be signalled or wrapper descendant teardown cannot be verified.
zxfer_signal_background_shell() {
	l_signal_shell_pid=$1
	l_signal_shell_scope=$2
	l_signal_shell_name=$3

	zxfer_is_uint "$l_signal_shell_pid" || return 0
	if [ "$l_signal_shell_scope" = pgid ]; then
		zxfer_signal_process_group "$l_signal_shell_name" "$l_signal_shell_pid" && return 0
		zxfer_signal_process_group 0 "$l_signal_shell_pid" && return 1
		# The parent can reach cleanup before setsid establishes the group.
		# Signal its still-owned launcher, then retry the group in case it
		# was created between the first group signal and the direct signal.
		# Only a launcher still in zxfer's own process group is that case;
		# any other holder of the PID is a recycled, unrelated process.
		if kill -s 0 "$l_signal_shell_pid" 2>/dev/null &&
			l_signal_shell_pgid=$(ps -o pgid= -p "$l_signal_shell_pid" 2>/dev/null) &&
			l_signal_shell_own_pgid=$(ps -o pgid= -p "$$" 2>/dev/null) &&
			zxfer_is_uint "${l_signal_shell_own_pgid##*[ ]}" &&
			[ "${l_signal_shell_pgid##*[ ]}" = "${l_signal_shell_own_pgid##*[ ]}" ]; then
			kill -s "$l_signal_shell_name" "$l_signal_shell_pid" 2>/dev/null || {
				! kill -s 0 "$l_signal_shell_pid" 2>/dev/null || return 1
			}
			zxfer_signal_process_group "$l_signal_shell_name" "$l_signal_shell_pid" && return 0
		fi
		! zxfer_signal_process_group 0 "$l_signal_shell_pid"
		return "$?"
	elif [ "$l_signal_shell_scope" = wrapper ] && [ "$l_signal_shell_name" = KILL ]; then
		zxfer_kill_background_wrapper "$l_signal_shell_pid"
		return "$?"
	else
		kill -s "$l_signal_shell_name" "$l_signal_shell_pid" 2>/dev/null && return 0
	fi
	! kill -s 0 "$l_signal_shell_pid" 2>/dev/null
}

# Purpose: Launch a pre-rendered internal shell command in the background,
# capture its output through the checked staging path, and register the
# child with the runtime cleanup registry.
# Usage: zxfer_execute_rendered_background_shell_command CMD OUTPUT_FILE
# [ERROR_FILE]. Only pipeline/operator renderers may call this API; a single
# helper and its arguments must use an argv-preserving execution path.
# Dry-run callers receive empty placeholder files so later tempfile consumers
# can continue without executing the background probe.
zxfer_execute_rendered_background_shell_command() {
	l_cmd=$1
	l_output_file=$2
	l_error_file=${3:-}

	zxfer_echoV "Executing command in the background: $l_cmd"
	zxfer_record_last_command_string "$l_cmd"
	if [ "${g_option_n_dryrun:-0}" -eq 1 ]; then
		zxfer_echoV "Dry run: $l_cmd"
		g_last_background_pid=""
		if ! zxfer_write_runtime_artifact_file "$l_output_file" ""; then
			return 1
		fi
		if [ -n "$l_error_file" ]; then
			if ! zxfer_write_runtime_artifact_file "$l_error_file" ""; then
				zxfer_cleanup_runtime_artifact_path "$l_output_file"
				return 1
			fi
		fi
		return 0
	fi
	zxfer_spawn_background_shell "$l_cmd" "$l_output_file" "$l_error_file" || return "$?"
	if ! zxfer_register_cleanup_pid \
		"$g_last_background_pid" "background command helper" \
		"$g_zxfer_background_shell_scope"; then
		if ! zxfer_abort_direct_child_pid \
			"$g_last_background_pid" TERM "background command helper" \
			"$g_zxfer_background_shell_scope"; then
			# Keep the child PID and scope registered so the ordered trap
			# path can retry teardown.
			return 1
		fi
		# A group leader can exit on TERM while descendants keep running.
		zxfer_cleanup_pid_abort_grace_wait
		if ! zxfer_abort_direct_child_pid \
			"$g_last_background_pid" KILL "background command helper" \
			"$g_zxfer_background_shell_scope"; then
			return 1
		fi
		wait "$g_last_background_pid" 2>/dev/null || :
		zxfer_unregister_cleanup_pid "$g_last_background_pid"
		g_last_background_pid=""
		return 1
	fi
}
