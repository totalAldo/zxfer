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
#   g_zxfer_original_invocation, and g_zxfer_report_fast_path (set when the
#   module is sourced).
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
}

# Purpose: Record the launcher argv as the failure report's invocation field.
# Usage: zxfer_set_original_invocation "$0" "$@"; stores "[redacted]" unless
# ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS is enabled.
zxfer_set_original_invocation() {
	if zxfer_failure_report_uses_unsafe_command_fields; then
		g_zxfer_original_invocation=$(zxfer_quote_command_argv "$@")
	else
		g_zxfer_original_invocation="[redacted]"
	fi
}

# Purpose: Print one operator-facing line to stderr.
# Usage: zxfer_warn_stderr TEXT...
zxfer_warn_stderr() {
	printf '%s\n' "$*" >&2
}

# The report fast paths below skip awk and sed for printable words, which
# needs [[:print:]] in case patterns. posh reads [![:print:]] as a plain
# bracket expression and would let control bytes through, so detect class
# support once here; without it every word takes the slow path.
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

# Purpose: Make a report value inert on terminals, pagers, and ZXFER_ERROR_LOG:
# backslash, tab, CR, and newline become \\ \t \r \n; other C0 bytes and DEL
# become \xHH.
# Usage: l_safe=$(zxfer_escape_report_value "$l_value")
zxfer_escape_report_value() {
	# Printable text without backslashes needs no escaping, so skip the awk
	# pipeline. [:print:] follows the shell's locale; the slow path below
	# passes bytes >= 0x80 through unchanged either way.
	case $1 in
	*[![:print:]]* | *\\*) ;;
	*)
		if [ "$g_zxfer_report_fast_path" = 1 ]; then
			printf '%s' "$1"
			return 0
		fi
		;;
	esac

	# Command substitution in the caller strips trailing newlines, so count
	# them here and append their escaped form after awk.
	l_report_value=$1
	l_trailing_newlines=0
	l_scan_value=$l_report_value
	while :; do
		case $l_scan_value in
		*'
')
			l_trailing_newlines=$((l_trailing_newlines + 1))
			l_scan_value=${l_scan_value%?}
			;;
		*)
			break
			;;
		esac
	done

	# shellcheck disable=SC2016
	printf '%s' "$l_report_value" | LC_ALL=C ${g_cmd_awk:-awk} '
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
'
	while [ "$l_trailing_newlines" -gt 0 ]; do
		printf '\\n'
		l_trailing_newlines=$((l_trailing_newlines - 1))
	done
}

# Purpose: Render one token as an escaped, single-quoted report word.
# Usage: l_word=$(zxfer_quote_token_for_report "$l_token")
zxfer_quote_token_for_report() {
	# Printable tokens without backslashes or single quotes quote as-is.
	case $1 in
	*[![:print:]]* | *\\* | *\'*) ;;
	*)
		if [ "$g_zxfer_report_fast_path" = 1 ]; then
			printf "'%s'" "$1"
			return 0
		fi
		;;
	esac
	l_value_escaped=$(zxfer_escape_report_value "$1")
	# LC_ALL=C: a UTF-8 sed rejects invalid multibyte input and prints nothing.
	l_value_safe=$(printf '%s' "$l_value_escaped" | LC_ALL=C sed "s/'/'\"'\"'/g")
	printf "'%s'" "$l_value_safe"
}

# Purpose: Render argv as space-separated report words, one per argument.
# Usage: l_cmd=$(zxfer_quote_command_argv "$@")
zxfer_quote_command_argv() {
	l_output=""
	for l_arg in "$@"; do
		# Same fast path as zxfer_quote_token_for_report, without its subshell.
		case $l_arg in
		*[![:print:]]* | *\\* | *\'*)
			l_quoted_arg=$(zxfer_quote_token_for_report "$l_arg")
			;;
		*)
			l_quoted_arg="'$l_arg'"
			[ "$g_zxfer_report_fast_path" = 1 ] ||
				l_quoted_arg=$(zxfer_quote_token_for_report "$l_arg")
			;;
		esac
		if [ "$l_output" = "" ]; then
			l_output=$l_quoted_arg
		else
			l_output="$l_output $l_quoted_arg"
		fi
	done
	printf '%s\n' "$l_output"
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

# Purpose: Render an optional shell-ready prefix plus quoted argv as one line,
# the format of dry-run output and failure reports.
# Usage: l_cmd=$(zxfer_render_command_for_report PREFIX [ARG...]); PREFIX may
# be empty.
zxfer_render_command_for_report() {
	l_prefix=$1
	shift

	if [ $# -gt 0 ]; then
		l_quoted_args=$(zxfer_quote_command_argv "$@")
	else
		l_quoted_args=""
	fi

	if [ "$l_prefix" != "" ] && [ "$l_quoted_args" != "" ]; then
		printf '%s %s\n' "$l_prefix" "$l_quoted_args"
	elif [ "$l_prefix" != "" ]; then
		printf '%s\n' "$l_prefix"
	else
		printf '%s\n' "$l_quoted_args"
	fi
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
		g_zxfer_failure_last_command=$(zxfer_escape_report_value "$1")
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
		g_zxfer_failure_last_command=$(zxfer_quote_command_argv "$@")
	else
		zxfer_record_last_command_opaque
	fi
}

# Purpose: Print one "key: value" report line with the value escaped.
# Usage: zxfer_append_report_field KEY VALUE; prints nothing for an empty VALUE.
zxfer_append_report_field() {
	[ -n "$2" ] || return 0
	printf '%s: %s\n' "$1" "$(zxfer_escape_report_value "$2")"
}

# Purpose: Print one "key: value" report line whose value is already escaped.
# Usage: zxfer_append_preescaped_report_field KEY VALUE; prints nothing for an
# empty VALUE.
zxfer_append_preescaped_report_field() {
	[ -n "$2" ] || return 0
	printf '%s: %s\n' "$1" "$2"
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
	zxfer_append_preescaped_report_field invocation "$l_report_invocation"
	zxfer_append_preescaped_report_field last_command "$l_report_last_command"
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

	l_errlog_parent=$(zxfer_get_path_parent_dir "$l_errlog_target")
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
# not a 0600 file owned by root or the effective user, or has another hard
# link.
# Usage: zxfer_validate_existing_error_log_file PATH; warns and returns 1 on
# refusal.
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
	if ! l_validate_owner_uid=$(zxfer_get_path_owner_uid "$l_validate_path"); then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_path\" because its owner could not be determined."
		return 1
	fi
	if ! zxfer_backup_owner_uid_is_allowed "$l_validate_owner_uid"; then
		l_validate_expected_owner_desc=$(zxfer_describe_expected_backup_owner)
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_path\" because it is owned by UID $l_validate_owner_uid instead of $l_validate_expected_owner_desc."
		return 1
	fi
	if ! l_validate_mode=$(zxfer_get_path_mode_octal "$l_validate_path"); then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_path\" because its permissions could not be determined."
		return 1
	fi
	if [ "$l_validate_mode" != "600" ]; then
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_path\" because its permissions ($l_validate_mode) are not 0600."
		return 1
	fi

	# The report would also land in every other name of this file, such as a
	# root-owned 0600 file that another user hard-linked into a shared sticky
	# parent. Field 2 of ls -ldn is the link count on every supported ls.
	l_validate_ls=$(ls -ldn "$l_validate_path" 2>/dev/null) || l_validate_ls=""
	IFS=' 	' read -r l_validate_ls_perm l_validate_links l_validate_ls_rest <<EOF
$l_validate_ls
EOF
	case $l_validate_links in
	1) ;;
	'' | *[!0-9]*)
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_path\" because its link count could not be determined."
		return 1
		;;
	*)
		zxfer_warn_stderr "zxfer: warning: refusing ZXFER_ERROR_LOG file \"$l_validate_path\" because it has $l_validate_links hard links."
		return 1
		;;
	esac
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
	if [ ! -e "$l_errlog_path" ] && [ ! -L "$l_errlog_path" ]; then
		# Exclusive creation under umask 077 and noclobber, in a subshell that
		# checks again just before its open: an entry that appeared since the
		# check above is not opened here, and the validation below appends to
		# another run's new log or refuses anything else.
		if (
			umask 077
			set -C
			[ ! -e "$l_errlog_path" ] && [ ! -L "$l_errlog_path" ] &&
				printf '' >"$l_errlog_path"
		) 2>/dev/null; then
			# A default ACL on the parent can override the umask.
			chmod 600 "$l_errlog_path" 2>/dev/null || :
		elif [ ! -e "$l_errlog_path" ] && [ ! -L "$l_errlog_path" ]; then
			zxfer_warn_stderr "zxfer: warning: unable to create ZXFER_ERROR_LOG file \"$l_errlog_path\"."
			return 1
		fi
	fi
	zxfer_validate_existing_error_log_file "$l_errlog_path" || return 1

	# One write per report. bash line-buffers its printf builtin, so a report
	# printed there would go out one line per write and could interleave with
	# a concurrent run's report. awk sends the whole report in one write when
	# it closes the file (a report larger than its output buffer, usually the
	# file system block size, takes more than one), and O_APPEND puts that
	# write at the end of the file.
	# shellcheck disable=SC2016 # awk reads ENVIRON; nothing is shell-expanded.
	if ! ZXFER_AWK_ERROR_LOG_PATH=$l_errlog_path \
		ZXFER_AWK_ERROR_LOG_REPORT=$1 \
		LC_ALL=C "${g_cmd_awk:-awk}" '
BEGIN {
	log_path = ENVIRON["ZXFER_AWK_ERROR_LOG_PATH"]
	printf "%s\n", ENVIRON["ZXFER_AWK_ERROR_LOG_REPORT"] >> log_path
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
		printf '%s: %s\n' "$1" "$(zxfer_escape_report_value "$2")" >&2
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
