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
# REPORTING / FAILURE HANDLING
################################################################################

# Module contract:
# owns globals: g_zxfer_failure_* structured failure context,
#   g_zxfer_original_invocation, g_zxfer_report_fast_path (set when the module
#   is sourced), and the result globals g_zxfer_escaped_report_value_result,
#   g_zxfer_quoted_report_token_result, g_zxfer_quoted_command_argv_result,
#   and g_zxfer_rendered_report_command_result.
# reads globals: g_option_* verbosity, beep, host, and mode flags;
#   g_zxfer_version; g_cmd_awk; ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS;
#   ZXFER_ERROR_LOG.
# mutates caches: none; zxfer_emit_failure_report appends each report to the
#   ZXFER_ERROR_LOG file, creating that file when it is missing.
# returns via stdout: escaped values, quoted commands, and rendered failure
#   reports.
#
# The failure context may be unset before zxfer_reset_session_state resets it, so
# readers apply the defaults at the point of use (stage "startup", every other
# field empty).

# Purpose: Clear the structured failure context for a new run.
# Usage: zxfer_reset_failure_context [STAGE]; keeps the launcher-captured
# g_zxfer_original_invocation.
zxfer_reset_failure_context() {
	g_zxfer_failure_report_emitted=0
	g_zxfer_failure_class=""
	g_zxfer_failure_stage=${1:-startup}
	g_zxfer_failure_message=""
	g_zxfer_failure_source_root=""
	g_zxfer_failure_current_source=""
	g_zxfer_failure_destination_root=""
	g_zxfer_failure_current_destination=""
	g_zxfer_failure_last_command=""
	g_zxfer_escaped_report_value_result=""
	g_zxfer_quoted_report_token_result=""
	g_zxfer_quoted_command_argv_result=""
	g_zxfer_rendered_report_command_result=""
}

# Purpose: Record the launcher argv as the failure report's invocation field.
# Usage: zxfer_set_original_invocation "$0" "$@"; stores "[redacted]" unless
# ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS is enabled.
zxfer_set_original_invocation() {
	if zxfer_failure_report_uses_unsafe_command_fields; then
		zxfer_quote_command_argv_into_result "$@" ||
			g_zxfer_quoted_command_argv_result=""
		g_zxfer_original_invocation=$g_zxfer_quoted_command_argv_result
	else
		g_zxfer_original_invocation="[redacted]"
	fi
}

# Purpose: Print one operator-facing line to stderr.
# Usage: zxfer_warn_stderr TEXT...
zxfer_warn_stderr() {
	printf '%s\n' "$*" >&2
}

# The report fast paths below skip awk for printable words, which needs
# [[:print:]] in case patterns. posh reads [![:print:]] as a plain bracket
# expression and would let control bytes through, so detect class support once
# here; without it every word takes the awk path.
# shellcheck disable=SC2194 # The constant word is the probe.
case 'a
b' in
*[![:print:]]*)
	g_zxfer_report_fast_path=1
	;;
*)
	g_zxfer_report_fast_path=0
	;;
esac

# Purpose: Escape VALUE for a report, and optionally single-quote it, in one
# exported LC_ALL=C scope on the in-shell fast path; restores LC_ALL before
# any awk fallback so no external command inherits the temporary export.
# Usage: zxfer_escape_or_quote_report_value VALUE QUOTE(0|1); publishes
# g_zxfer_escaped_report_value_result (and g_zxfer_quoted_report_token_result
# when QUOTE is 1).
zxfer_escape_or_quote_report_value() {
	g_zxfer_escaped_report_value_result=""
	[ "$2" -eq 0 ] || g_zxfer_quoted_report_token_result=""
	l_report_escape_had_lc_all=0
	if [ "${LC_ALL+set}" = set ]; then
		l_report_escape_had_lc_all=1
		l_report_escape_saved_lc_all=$LC_ALL
	fi
	l_report_escape_fast_ok=0
	if [ "$g_zxfer_report_fast_path" = 1 ]; then
		# Export LC_ALL so FreeBSD /bin/sh applies C even when LANG is exported.
		LC_ALL=C
		export LC_ALL
		# Printable ASCII without backslashes needs no escaping; bounded
		# values containing only printable ASCII plus \, TAB, CR, or LF are
		# escaped in-shell. Checking under LC_ALL=C keeps multibyte locales
		# from hiding 0x5C ('\') as a trail byte; any remaining non-printable
		# byte (other C0, DEL, or bytes >= 0x80) falls back to awk on $1.
		case $1 in
		*[![:print:]]* | *\\*)
			if [ "${#1}" -le 4096 ]; then
				# Escape backslashes first so \t, \r and \n markers inserted
				# below are never escaped a second time.
				zxfer_replace_all_into_result "$1" "\\" "\\\\"
				zxfer_replace_all_into_result "$g_zxfer_replace_all_result" "$ZXFER_TAB" '\t'
				zxfer_replace_all_into_result "$g_zxfer_replace_all_result" "$ZXFER_CR" '\r'
				zxfer_replace_all_into_result "$g_zxfer_replace_all_result" "$ZXFER_LF" '\n'
				case $g_zxfer_replace_all_result in
				*[![:print:]]*) ;;
				*)
					l_report_escape_val=$g_zxfer_replace_all_result
					l_report_escape_fast_ok=1
					;;
				esac
			fi
			;;
		*)
			l_report_escape_val=$1
			l_report_escape_fast_ok=1
			;;
		esac
		if [ "$l_report_escape_fast_ok" -eq 1 ] && [ "$2" -eq 1 ]; then
			zxfer_replace_all_into_result "$l_report_escape_val" "'" "'\"'\"'"
		fi
		if [ "$l_report_escape_had_lc_all" -eq 1 ]; then
			LC_ALL=$l_report_escape_saved_lc_all
		else
			unset LC_ALL
		fi
		if [ "$l_report_escape_fast_ok" -eq 1 ]; then
			g_zxfer_escaped_report_value_result=$l_report_escape_val
			[ "$2" -eq 0 ] || g_zxfer_quoted_report_token_result="'$g_zxfer_replace_all_result'"
			return 0
		fi
	fi

	# Terminate the final record, even if it is empty. Each separator
	# between awk records then represents exactly one newline in the value;
	# ORS is empty so command substitution strips nothing.
	# shellcheck disable=SC2016
	l_report_escape_val=$(printf '%s\n' "$1" | LC_ALL=C "${g_cmd_awk:-awk}" '
BEGIN {
	ORS = ""
	for (i = 1; i < 32; i++) {
		ctrl[sprintf("%c", i)] = sprintf("\\x%02X", i)
	}
	ctrl[sprintf("%c", 9)] = "\\t"
	ctrl[sprintf("%c", 13)] = "\\r"
	ctrl[sprintf("%c", 127)] = "\\x7F"
}
{
	if (NR > 1) {
		printf "\\n"
	}
	line = $0
	for (i = 1; i <= length(line); i++) {
		c = substr(line, i, 1)
		if (c == "\\") {
			printf "\\\\"
		} else if (c in ctrl) {
			printf "%s", ctrl[c]
		} else {
			printf "%s", c
		}
	}
}
') || return "$?"
	g_zxfer_escaped_report_value_result=$l_report_escape_val
	if [ "$2" -eq 1 ]; then
		LC_ALL=C
		export LC_ALL
		zxfer_replace_all_into_result "$l_report_escape_val" "'" "'\"'\"'"
		if [ "$l_report_escape_had_lc_all" -eq 1 ]; then
			LC_ALL=$l_report_escape_saved_lc_all
		else
			unset LC_ALL
		fi
		g_zxfer_quoted_report_token_result="'$g_zxfer_replace_all_result'"
	fi
}

# Purpose: Escape a report value in the current shell: backslash, tab, CR, and
# newline become \\ \t \r \n; other C0 bytes and DEL become \xHH.
# Usage: zxfer_escape_report_value_into_result VALUE; publishes
# g_zxfer_escaped_report_value_result.
zxfer_escape_report_value_into_result() {
	zxfer_escape_or_quote_report_value "$1" 0
}

# Purpose: Print an escaped report value to stdout.
# Usage: l_safe=$(zxfer_escape_report_value "$l_value")
zxfer_escape_report_value() {
	zxfer_escape_report_value_into_result "$1" || return
	printf '%s' "$g_zxfer_escaped_report_value_result"
}

# Purpose: Render one token as an escaped, single-quoted report word in the
# current shell without invoking sed.
# Usage: zxfer_quote_report_token_into_result TOKEN; publishes
# g_zxfer_quoted_report_token_result.
zxfer_quote_report_token_into_result() {
	zxfer_escape_or_quote_report_value "$1" 1
}

# Purpose: Render one token as an escaped, single-quoted report word.
# Usage: l_word=$(zxfer_quote_token_for_report "$l_token")
zxfer_quote_token_for_report() {
	zxfer_quote_report_token_into_result "$1" || return
	printf '%s' "$g_zxfer_quoted_report_token_result"
}

# Purpose: Render argv as space-separated report words in the current shell.
# Usage: zxfer_quote_command_argv_into_result [ARG...]; publishes
# g_zxfer_quoted_command_argv_result.
zxfer_quote_command_argv_into_result() {
	g_zxfer_quoted_command_argv_result=""
	l_quote_argv_output=""
	for l_quote_argv_arg in "$@"; do
		zxfer_quote_report_token_into_result "$l_quote_argv_arg" || return
		l_quote_argv_output=${l_quote_argv_output:+$l_quote_argv_output }$g_zxfer_quoted_report_token_result
	done
	g_zxfer_quoted_command_argv_result=$l_quote_argv_output
}

# Purpose: Report whether failure reports may show verbatim command strings.
# Usage: zxfer_failure_report_uses_unsafe_command_fields && ...; true when
# ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS is 1/yes/true/on (any case).
zxfer_failure_report_uses_unsafe_command_fields() {
	case "${ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS:-}" in
	1 | [Yy][Ee][Ss] | [Tt][Rr][Uu][Ee] | [Oo][Nn])
		return 0
		;;
	esac

	return 1
}

# Purpose: Report whether a rendered display command has any reader (-v, -V,
# or unsafe failure-report mode).
# Usage: if zxfer_command_display_render_enabled; then render and record it;
# else zxfer_record_last_command_opaque; fi
zxfer_command_display_render_enabled() {
	[ "${g_option_v_verbose:-0}" -eq 1 ] && return 0
	[ "${g_option_V_very_verbose:-0}" -eq 1 ] && return 0
	zxfer_failure_report_uses_unsafe_command_fields
}

# Purpose: Report whether a rendered trace command has a reader: -V prints it
# and unsafe failure-report mode records it; plain -v shows neither.
# Usage: if zxfer_command_trace_enabled; then zxfer_trace_rendered_command
# LABEL "$(render ...)"; else zxfer_record_last_command_opaque; fi
zxfer_command_trace_enabled() {
	[ "${g_option_V_very_verbose:-0}" -eq 1 ] ||
		zxfer_failure_report_uses_unsafe_command_fields
}

# Purpose: Print "LABEL: CMD" under -V and record CMD as the last command.
# Usage: zxfer_trace_rendered_command "Running command" "$l_rendered_cmd"
zxfer_trace_rendered_command() {
	zxfer_echoV "$1: $2"
	zxfer_record_last_command_string "$2"
}

# Purpose: Record the redacted last-command marker without rendering anything.
# Usage: Called on quiet paths in place of zxfer_record_last_command_string.
zxfer_record_last_command_opaque() {
	g_zxfer_failure_last_command="[redacted]"
}

# Purpose: Render an optional shell-ready prefix plus quoted argv as one line
# in the current shell, the format of dry-run output and failure reports.
# Usage: zxfer_render_command_for_report_into_result PREFIX [ARG...]; PREFIX
# may be empty; publishes g_zxfer_rendered_report_command_result.
zxfer_render_command_for_report_into_result() {
	g_zxfer_rendered_report_command_result=""
	l_render_report_prefix=${1:-}
	[ $# -eq 0 ] || shift

	if [ $# -gt 0 ]; then
		zxfer_quote_command_argv_into_result "$@" || return
		l_render_report_quoted_args=$g_zxfer_quoted_command_argv_result
	else
		l_render_report_quoted_args=""
	fi

	if [ "$l_render_report_prefix" != "" ] && [ "$l_render_report_quoted_args" != "" ]; then
		g_zxfer_rendered_report_command_result="$l_render_report_prefix $l_render_report_quoted_args"
	elif [ "$l_render_report_prefix" != "" ]; then
		g_zxfer_rendered_report_command_result=$l_render_report_prefix
	else
		g_zxfer_rendered_report_command_result=$l_render_report_quoted_args
	fi
}

# Purpose: Render an optional shell-ready prefix plus quoted argv as one line,
# the format of dry-run output and failure reports.
# Usage: l_cmd=$(zxfer_render_command_for_report PREFIX [ARG...]); PREFIX may
# be empty.
zxfer_render_command_for_report() {
	zxfer_render_command_for_report_into_result "$@" || return
	printf '%s\n' "$g_zxfer_rendered_report_command_result"
}

# Purpose: Name the replication stage that a later failure report shows.
# Usage: zxfer_set_failure_stage STAGE; an empty STAGE is ignored.
zxfer_set_failure_stage() {
	[ -z "${1:-}" ] || g_zxfer_failure_stage=$1
}

# Purpose: Set the failure class ahead of a throw whose category is more
# specific than the runtime default.
# Usage: zxfer_set_failure_class usage|dependency|runtime|""; returns 1 for any
# other class.
zxfer_set_failure_class() {
	case ${1:-} in
	usage | dependency | runtime | '')
		g_zxfer_failure_class=${1:-}
		;;
	*)
		return 1
		;;
	esac
}

# Purpose: Publish a cleanup failure unless an earlier failure message already
# owns the report (first failure wins).
# Usage: zxfer_set_failure_context_if_empty CLASS STAGE MESSAGE from EXIT
# cleanup, where throwing is not allowed.
zxfer_set_failure_context_if_empty() {
	[ -z "${g_zxfer_failure_message:-}" ] || return 0
	zxfer_set_failure_class "${1:-runtime}" || return 1
	g_zxfer_failure_stage=${2:-trap cleanup}
	g_zxfer_failure_message=${3:-}
}

# Purpose: Record the replication source and destination roots for reports.
# Usage: zxfer_set_failure_roots [SOURCE_ROOT] [DESTINATION_ROOT]; empty
# values keep the previous root.
zxfer_set_failure_roots() {
	[ -z "${1:-}" ] || g_zxfer_failure_source_root=$1
	[ -z "${2:-}" ] || g_zxfer_failure_destination_root=$2
}

# Purpose: Record the dataset pair being replicated for reports.
# Usage: zxfer_set_current_dataset_context [SOURCE] [DESTINATION]; empty
# values keep the previous dataset.
zxfer_set_current_dataset_context() {
	[ -z "${1:-}" ] || g_zxfer_failure_current_source=$1
	[ -z "${2:-}" ] || g_zxfer_failure_current_destination=$2
}

# Purpose: Record a rendered command string as the report's last command.
# Usage: zxfer_record_last_command_string CMD; stores the escaped CMD only in
# unsafe report mode, "[redacted]" otherwise, and "" for an empty CMD.
zxfer_record_last_command_string() {
	if [ $# -eq 0 ] || [ "$1" = "" ]; then
		g_zxfer_failure_last_command=""
	elif zxfer_failure_report_uses_unsafe_command_fields; then
		zxfer_escape_report_value_into_result "$1" ||
			g_zxfer_escaped_report_value_result=""
		g_zxfer_failure_last_command=$g_zxfer_escaped_report_value_result
	else
		zxfer_record_last_command_opaque
	fi
}

# Purpose: Record an argv as the report's last command.
# Usage: zxfer_record_last_command_argv ARG...; stores the quoted argv only in
# unsafe report mode, "[redacted]" otherwise, and "" for an empty argv.
zxfer_record_last_command_argv() {
	if [ $# -eq 0 ]; then
		g_zxfer_failure_last_command=""
	elif zxfer_failure_report_uses_unsafe_command_fields; then
		zxfer_quote_command_argv_into_result "$@" ||
			g_zxfer_quoted_command_argv_result=""
		g_zxfer_failure_last_command=$g_zxfer_quoted_command_argv_result
	else
		zxfer_record_last_command_opaque
	fi
}

# Purpose: Print one "key: value" report line with the value escaped.
# Usage: zxfer_append_report_field KEY VALUE; prints nothing for an empty VALUE.
zxfer_append_report_field() {
	[ -n "$2" ] || return 0
	zxfer_escape_report_value_into_result "$2"
	printf '%s: %s\n' "$1" "$g_zxfer_escaped_report_value_result"
}

# Purpose: Render the structured failure report for one exit status.
# Usage: l_report=$(zxfer_render_failure_report EXIT_STATUS); command fields
# show "[redacted]" unless unsafe report mode is enabled.
zxfer_render_failure_report() {
	l_render_exit_status=$1

	l_timestamp=$(date '+%Y-%m-%dT%H:%M:%S%z' 2>/dev/null || date)
	l_hostname=$(uname -n 2>/dev/null || hostname 2>/dev/null || echo unknown)
	l_failure_class=${g_zxfer_failure_class:-}
	if [ -z "$l_failure_class" ]; then
		if [ "$l_render_exit_status" -eq 2 ]; then
			l_failure_class=usage
		else
			l_failure_class=runtime
		fi
	fi
	l_failure_message=${g_zxfer_failure_message:-}
	if [ -z "$l_failure_message" ]; then
		l_failure_message="zxfer exited with status $l_render_exit_status."
	fi
	l_mode=""
	if [ -n "${g_option_R_recursive:-}" ]; then
		l_mode=recursive
	elif [ -n "${g_option_N_nonrecursive:-}" ]; then
		l_mode=nonrecursive
	fi
	l_report_invocation=${g_zxfer_original_invocation:-}
	l_report_last_command=${g_zxfer_failure_last_command:-}
	if ! zxfer_failure_report_uses_unsafe_command_fields; then
		[ -z "$l_report_invocation" ] || l_report_invocation="[redacted]"
		[ -z "$l_report_last_command" ] || l_report_last_command="[redacted]"
	fi

	printf 'zxfer: failure report begin\n'
	zxfer_append_report_field timestamp "$l_timestamp"
	zxfer_append_report_field hostname "$l_hostname"
	zxfer_append_report_field zxfer_version "${g_zxfer_version:-unknown}"
	zxfer_append_report_field exit_status "$l_render_exit_status"
	zxfer_append_report_field failure_class "$l_failure_class"
	zxfer_append_report_field failure_stage "${g_zxfer_failure_stage:-startup}"
	zxfer_append_report_field message "$l_failure_message"
	zxfer_append_report_field source_root "${g_zxfer_failure_source_root:-}"
	zxfer_append_report_field current_source "${g_zxfer_failure_current_source:-}"
	zxfer_append_report_field destination_root "${g_zxfer_failure_destination_root:-}"
	zxfer_append_report_field current_destination "${g_zxfer_failure_current_destination:-}"
	zxfer_append_report_field origin_host "${g_option_O_origin_host:-}"
	zxfer_append_report_field target_host "${g_option_T_target_host:-}"
	zxfer_append_report_field dry_run "${g_option_n_dryrun:-0}"
	zxfer_append_report_field mode "$l_mode"
	zxfer_append_report_field yield_iterations "${g_option_Y_yield_iterations:-}"
	[ -z "$l_report_invocation" ] || printf 'invocation: %s\n' "$l_report_invocation"
	[ -z "$l_report_last_command" ] || printf 'last_command: %s\n' "$l_report_last_command"
	printf 'zxfer: failure report end\n'
}

################################################################################
# ZXFER_ERROR_LOG MIRROR
################################################################################

# ZXFER_ERROR_LOG names an operator-chosen file outside the run root. Each
# report is appended with one O_APPEND write, which the kernel places at the
# end of the file as one unit, so concurrent runs need no lock. The checks keep
# other users out: no path component may be a symlink, the parent must be
# owned by root or the effective user and not writable by others unless it is
# sticky, and the log must be a regular 0600 file with a single link, owned by
# root or the effective user. They do not stop root or the effective user from
# replacing the log between the checks and the write.

# Purpose: Refuse a ZXFER_ERROR_LOG path that is not absolute, passes through
# a symlink, or whose parent directory is missing or untrusted.
# Usage: zxfer_validate_error_log_parent PATH; warns and returns 1 on refusal.
zxfer_validate_error_log_parent() {
	l_errlog_target=$1

	case $l_errlog_target in
	/*) ;;
	*)
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_errlog_target\" because it is not absolute."
		return 1
		;;
	esac

	if l_errlog_symlink_component=$(zxfer_find_symlink_path_component "$l_errlog_target"); then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_errlog_target\" because path component \"$l_errlog_symlink_component\" is a symlink."
		return 1
	fi

	case $l_errlog_target in
	?*/*) l_errlog_parent=${l_errlog_target%/*} ;;
	*) l_errlog_parent=/ ;;
	esac
	if [ ! -d "$l_errlog_parent" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_errlog_target\" because parent directory \"$l_errlog_parent\" does not exist."
		return 1
	fi
	if ! zxfer_validate_temp_root_candidate "$l_errlog_parent" >/dev/null; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_errlog_target\" because parent directory \"$l_errlog_parent\" is not owned by root or the effective user, or is writable by others without sticky-bit protection."
		return 1
	fi
}

# Purpose: Refuse an existing log that is a symlink, is not a regular file, is
# not owned by root or the effective user, has another hard link, or is not
# mode 0600.
# Usage: zxfer_validate_existing_error_log_file PATH [CREATED]; warns and
# returns 1 on refusal. CREATED 1 marks a log this run has just created: it
# gets mode 0600 (a default ACL on the parent can override the umask) once
# the other checks pass, since chmod follows a symlink and the create step
# can open a FIFO or device that another user put at the name.
zxfer_validate_existing_error_log_file() {
	l_validate_path=$1

	if [ -L "$l_validate_path" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_validate_path\" because it is a symlink."
		return 1
	fi
	if [ -e "$l_validate_path" ] && [ ! -f "$l_validate_path" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG path \"$l_validate_path\" because it is not a regular file."
		return 1
	fi
	if ! zxfer_read_path_metadata "$l_validate_path"; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_path\" because its owner could not be determined."
		return 1
	fi
	l_validate_owner_uid=$g_zxfer_path_owner_uid_result
	l_validate_links=$g_zxfer_path_link_count_result
	l_validate_mode=$g_zxfer_path_mode_result
	if ! zxfer_backup_owner_uid_is_allowed "$l_validate_owner_uid"; then
		l_validate_expected_owner_desc=$(zxfer_describe_expected_backup_owner)
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_path\" because it is owned by UID $l_validate_owner_uid instead of $l_validate_expected_owner_desc."
		return 1
	fi

	# The report would also land in every other name of this file, such as a
	# root-owned 0600 file that another user hard-linked into a shared sticky
	# parent. Owner, link count and mode come from the same metadata read.
	case $l_validate_links in
	1) ;;
	'' | *[!0123456789]*)
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_path\" because its link count could not be determined."
		return 1
		;;
	*)
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_path\" because it has $l_validate_links hard links."
		return 1
		;;
	esac

	# Now a single-link regular file owned by root or the effective user,
	# which other users cannot replace in a parent that passed its checks.
	if [ "${2:-0}" = 1 ]; then
		chmod 600 "$l_validate_path" 2>/dev/null || :
		# A default ACL can widen creation permissions; verify the chmod's
		# result with a fresh read rather than the pre-chmod record.
		if [ -e "$l_validate_path" ] && zxfer_read_path_metadata "$l_validate_path"; then
			l_validate_mode=$g_zxfer_path_mode_result
		else
			l_validate_mode=""
		fi
	fi
	if [ -z "$l_validate_mode" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_path\" because its permissions could not be determined."
		return 1
	fi
	if [ "$l_validate_mode" != "600" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_path\" because its permissions ($l_validate_mode) are not 0600."
		return 1
	fi
}

# Purpose: Append one failure report to ZXFER_ERROR_LOG, first creating a
# private 0600 log when none exists.
# Usage: zxfer_append_failure_report_to_log REPORT; a no-op when
# ZXFER_ERROR_LOG is unset, otherwise warns and returns 1 on any refusal or
# write failure. The caller ignores the status, so a log problem never changes
# zxfer's exit status or its stderr report.
zxfer_append_failure_report_to_log() {
	l_errlog_path=${ZXFER_ERROR_LOG:-}

	[ -n "$l_errlog_path" ] || return 0
	zxfer_validate_error_log_parent "$l_errlog_path" || return 1
	l_errlog_created=0
	if [ ! -e "$l_errlog_path" ] && [ ! -L "$l_errlog_path" ]; then
		# Exclusive creation under umask 077 and noclobber, in a subshell that
		# checks again just before its open. noclobber refuses an existing
		# regular file but opens a FIFO or device, even through a symlink, so
		# the second check narrows the window in which another user can put
		# one at the name in a shared sticky parent; a FIFO put there in time
		# blocks the open until someone reads it. The validation below refuses
		# such an entry and appends to a log another run has just created.
		if (
			umask 077
			set -C
			[ ! -e "$l_errlog_path" ] && [ ! -L "$l_errlog_path" ] &&
				printf '' >"$l_errlog_path"
		) 2>/dev/null; then
			l_errlog_created=1
		elif [ ! -e "$l_errlog_path" ] && [ ! -L "$l_errlog_path" ]; then
			zxfer_warn_stderr "zxfer: warning: unable to create ZXFER_ERROR_LOG file \"$l_errlog_path\"."
			return 1
		fi
	fi
	zxfer_validate_existing_error_log_file "$l_errlog_path" "$l_errlog_created" || return 1

	# One write per report. bash line-buffers its printf builtin, so a report
	# printed there would go out one line per write and could interleave with
	# a concurrent run's report. awk collects the report from the pipe and
	# sends it in one write when it closes the file (a report larger than its
	# output buffer, usually the file system block size, takes more than one),
	# and O_APPEND puts that write at the end of the file. The pipe, unlike an
	# environment variable, has no size limit.
	# shellcheck disable=SC2016 # awk reads ENVIRON; nothing is shell-expanded.
	if ! printf '%s\n' "$1" |
		ZXFER_AWK_ERROR_LOG_PATH=$l_errlog_path LC_ALL=C "${g_cmd_awk:-awk}" '
{ report = report $0 "\n" }
END {
	log_path = ENVIRON["ZXFER_AWK_ERROR_LOG_PATH"]
	printf "%s", report >> log_path
	exit (close(log_path) != 0)
}'; then
		zxfer_warn_stderr "zxfer: warning: unable to append failure report to ZXFER_ERROR_LOG file \"$l_errlog_path\"."
		return 1
	fi
}

# Purpose: Print the failure report for a non-zero exit once, then mirror it to
# ZXFER_ERROR_LOG.
# Usage: zxfer_emit_failure_report EXIT_STATUS, from the EXIT trap.
zxfer_emit_failure_report() {
	l_emit_exit_status=$1

	[ "$l_emit_exit_status" -ne 0 ] || return 0
	[ "${g_zxfer_failure_report_emitted:-0}" -eq 0 ] || return 0

	l_emit_report=$(zxfer_render_failure_report "$l_emit_exit_status")
	printf '%s\n' "$l_emit_report" >&2
	# Mark before mirroring so a re-entered EXIT trap never prints it twice.
	g_zxfer_failure_report_emitted=1
	zxfer_append_failure_report_to_log "$l_emit_report" || true
}

# Purpose: Stop the run through the structured failure path: keep an earlier
# class (else runtime), record MSG, print it to stderr, beep, and exit.
# Usage: zxfer_throw_failure MSG STATUS [usage], the shared core of the three
# zxfer_throw_* helpers; "usage" prints "Error: MSG" (when MSG is set) plus
# the usage text.
zxfer_throw_failure() {
	l_throw_msg=$1
	l_throw_status=$2

	[ -n "${g_zxfer_failure_class:-}" ] || g_zxfer_failure_class=runtime
	[ -z "$l_throw_msg" ] || g_zxfer_failure_message=$l_throw_msg
	if [ "${3:-}" = usage ]; then
		[ -z "$l_throw_msg" ] || zxfer_warn_stderr "Error: $l_throw_msg"
		zxfer_usage >&2
	else
		zxfer_warn_stderr "$l_throw_msg"
	fi
	zxfer_beep "$l_throw_status"
	exit "$l_throw_status"
}

# Purpose: Stop the run with MSG on stderr and a structured failure report.
# Usage: zxfer_throw_error MSG [STATUS]; STATUS defaults to 1.
zxfer_throw_error() {
	zxfer_throw_failure "$1" "${2:-1}"
}

# Purpose: Stop the run with "Error: MSG", the usage text, and a structured
# failure report.
# Usage: zxfer_throw_error_with_usage MSG [STATUS]; STATUS defaults to 1.
zxfer_throw_error_with_usage() {
	zxfer_throw_failure "$1" "${2:-1}" usage
}

# Purpose: Stop the run on invalid CLI input as a usage-class failure.
# Usage: zxfer_throw_usage_error MSG [STATUS]; STATUS defaults to 2.
zxfer_throw_usage_error() {
	g_zxfer_failure_class=usage
	zxfer_throw_failure "$1" "${2:-2}" usage
}

# Purpose: Stop the run on a missing or unusable helper as a dependency-class
# failure.
# Usage: zxfer_throw_dependency_error MSG [STATUS]; STATUS defaults to 1.
zxfer_throw_dependency_error() {
	g_zxfer_failure_class=dependency
	zxfer_throw_error "$1" "${2:-1}"
}

# The verbose printers use printf, never echo: dash's and macOS /bin/sh's echo
# expand backslash escapes such as \033 and \c, which would turn escaped report
# text back into raw control bytes or cut the line.

# Purpose: Print progress text to stdout only under -v.
# Usage: zxfer_echov TEXT
zxfer_echov() {
	if [ "${g_option_v_verbose:-0}" -eq 1 ]; then
		printf '%s\n' "$*"
	fi
}

# Purpose: Print diagnostic text to stderr only under -V.
# Usage: zxfer_echoV TEXT
zxfer_echoV() {
	if [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
		printf '%s\n' "$*" >&2
	fi
}

# Purpose: Print "LABEL: VALUE" to stderr only under -V, with VALUE escaped as
# zxfer_escape_report_value does, for values that may hold untrusted bytes.
# Usage: zxfer_echoV_escaped LABEL VALUE
zxfer_echoV_escaped() {
	if [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
		zxfer_escape_report_value_into_result "$2"
		printf '%s: %s\n' "$1" "$g_zxfer_escaped_report_value_result" >&2
	fi
}

# Purpose: Play the -b/-B end-of-run beep: failure beeps with -b or -B,
# success beeps only with -B. FreeBSD speaker only; elsewhere it logs a -V
# note and returns.
# Usage: zxfer_beep EXIT_STATUS (defaults to 1, a failure).
zxfer_beep() {
	l_beep_exit_status=${1:-1}

	if [ "${g_option_b_beep_always:-0}" -ne 1 ] && [ "${g_option_B_beep_on_success:-0}" -ne 1 ]; then
		return
	fi

	# Speaker control is FreeBSD-specific; skip on other hosts so replication continues.
	l_os=$(uname 2>/dev/null || echo "unknown")
	if [ "$l_os" != "FreeBSD" ]; then
		zxfer_echoV "Beep requested but unsupported on $l_os; skipping."
		return
	fi

	if ! command -v kldstat >/dev/null 2>&1 || ! command -v kldload >/dev/null 2>&1; then
		zxfer_echoV "Beep requested but speaker tools are missing; skipping."
		return
	fi

	if ! [ -c /dev/speaker ]; then
		zxfer_echoV "Beep requested but /dev/speaker missing; skipping."
		return
	fi

	# load the speaker kernel module if not loaded already
	l_speaker_km_loaded=$(kldstat | grep -c speaker.ko)
	if [ "$l_speaker_km_loaded" = "0" ]; then
		if ! kldload "speaker" >/dev/null 2>&1; then
			zxfer_echoV "Unable to load speaker module; skipping beep."
			return
		fi
	fi

	# play the appropriate beep
	if [ "$l_beep_exit_status" -eq 0 ]; then
		if [ "$g_option_B_beep_on_success" -eq 1 ]; then
			echo "T255CCMLEG~EG..." >/dev/speaker 2>/dev/null ||
				zxfer_echoV "Success beep failed; skipping."
		fi
	else
		echo "T150A<C.." >/dev/speaker 2>/dev/null ||
			zxfer_echoV "Failure beep failed; skipping."
	fi
}
