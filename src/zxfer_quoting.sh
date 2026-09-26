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
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

# BSD HEADER END
# shellcheck shell=sh disable=SC2034,SC2154

################################################################################
# TOKEN VALIDATION AND COMMAND QUOTING
################################################################################

# Module contract:
# owns globals: the load-time constants ZXFER_TAB, ZXFER_LF and ZXFER_CR;
#   the zxfer_split_begin saves g_zxfer_split_saved_*; and the result globals
#   g_zxfer_split_tokens_result, g_zxfer_escaped_single_quotes_result,
#   g_zxfer_literal_token_error_result, g_zxfer_shell_command_result and
#   g_zxfer_stripped_path_result.
# reads globals: none. Each render counts itself through
#   zxfer_profile_increment_counter (zxfer_profile.sh).
# mutates caches: none. zxfer_split_begin changes IFS and noglob until
#   zxfer_split_end restores them.
# returns via stdout: none.
# No helper uses a command substitution, pipe, or subshell.

# Line-control bytes: ZXFER_TAB holds a literal tab and ZXFER_LF a literal
# newline. Carriage return needs printf, so this is the module's only command
# substitution, and it runs once, at load time.
ZXFER_TAB='	'
ZXFER_LF='
'
ZXFER_CR=$(printf '\r')

# Purpose: Check that a value holds no tab, carriage return, or newline byte.
# Usage: zxfer_value_is_single_line VALUE; returns 1 on such a byte, prints
# nothing, and leaves each caller its own error text.
zxfer_value_is_single_line() {
	case $1 in
	*"$ZXFER_TAB"* | *"$ZXFER_CR"* | *"$ZXFER_LF"*) return 1 ;;
	esac
}

# Purpose: Check that a value is a non-empty string of decimal digits.
# Usage: zxfer_is_uint VALUE; returns 1 otherwise and prints nothing.
zxfer_is_uint() {
	case $1 in
	'' | *[!0-9]*) return 1 ;;
	esac
}

# Purpose: Save the caller's IFS and noglob state, then set IFS and noglob for
# literal field splitting.
# Usage: zxfer_split_begin [IFS_VALUE] (default space, tab, newline), split,
# then zxfer_split_end. Not reentrant: there is one saved state, so never nest
# pairs or call a splitting helper between them.
zxfer_split_begin() {
	if [ "${IFS+set}" = set ]; then
		g_zxfer_split_saved_ifs_set=1
		g_zxfer_split_saved_ifs=$IFS
	else
		g_zxfer_split_saved_ifs_set=0
		g_zxfer_split_saved_ifs=""
	fi
	case $- in
	*f*) g_zxfer_split_saved_noglob=1 ;;
	*) g_zxfer_split_saved_noglob=0 ;;
	esac
	set -f
	IFS=${1-" $ZXFER_TAB$ZXFER_LF"}
}

# Purpose: Restore the IFS and noglob state saved by zxfer_split_begin.
# Usage: zxfer_split_end, once after each zxfer_split_begin.
zxfer_split_end() {
	if [ "$g_zxfer_split_saved_ifs_set" -eq 1 ]; then
		IFS=$g_zxfer_split_saved_ifs
	else
		unset IFS
	fi
	[ "$g_zxfer_split_saved_noglob" -eq 1 ] || set +f
}

# Purpose: Escape a value for reinsertion into a single-quoted shell string:
# each ' becomes '\''.
# Usage: zxfer_escape_single_quotes_into_result VALUE; publishes
# g_zxfer_escaped_single_quotes_result.
zxfer_escape_single_quotes_into_result() {
	g_zxfer_escaped_single_quotes_result=""
	l_escape_rest=$1
	while :; do
		case $l_escape_rest in
		*\'*) ;;
		*) break ;;
		esac
		g_zxfer_escaped_single_quotes_result="$g_zxfer_escaped_single_quotes_result${l_escape_rest%%\'*}'\\''"
		l_escape_rest=${l_escape_rest#*\'}
	done
	g_zxfer_escaped_single_quotes_result=$g_zxfer_escaped_single_quotes_result$l_escape_rest
}

# Purpose: Split a literal string on whitespace into newline-joined tokens.
# Usage: zxfer_split_tokens_into_result STRING; publishes
# g_zxfer_split_tokens_result. Not a shell parser: quotes stay literal.
zxfer_split_tokens_into_result() {
	# Each ; | & ends its token, so "a;b|c" splits as "a;" "b|" "c". The
	# bracket stays unescaped: ksh93 would also match a backslash in [;\|\&].
	l_split_rest=$1
	l_split_input=""
	while :; do
		l_split_tail=${l_split_rest#*[;|&]}
		[ "$l_split_tail" != "$l_split_rest" ] || break
		l_split_input=$l_split_input${l_split_rest%"$l_split_tail"}' '
		l_split_rest=$l_split_tail
	done
	l_split_input=$l_split_input$l_split_rest

	zxfer_split_begin
	# shellcheck disable=SC2086  # Intentional literal whitespace splitting.
	set -- $l_split_input
	# "$*" joins the tokens with the first IFS character.
	IFS=$ZXFER_LF
	g_zxfer_split_tokens_result="$*"
	zxfer_split_end
}

# Purpose: Reject token strings that would need shell quote or escape parsing.
# Usage: zxfer_check_literal_token_string STRING [LABEL]; on rejection returns
# 1 with the operator message in g_zxfer_literal_token_error_result.
zxfer_check_literal_token_string() {
	g_zxfer_literal_token_error_result=""
	case $1 in
	*\\* | *\"* | *\'*)
		g_zxfer_literal_token_error_result="${2:-command} must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
		return 1
		;;
	esac
}

# Purpose: Render arguments as one shell command, each argument single-quoted.
# Usage: zxfer_render_shell_command_from_argv ARG...; publishes
# g_zxfer_shell_command_result. For rendered-shell APIs and operator display,
# never as a substitute for direct-argv execution.
zxfer_render_shell_command_from_argv() {
	zxfer_profile_increment_counter g_zxfer_profile_command_render_calls
	g_zxfer_shell_command_result=""
	l_render_separator=""
	for l_render_arg in "$@"; do
		zxfer_escape_single_quotes_into_result "$l_render_arg"
		g_zxfer_shell_command_result="$g_zxfer_shell_command_result$l_render_separator'$g_zxfer_escaped_single_quotes_result'"
		l_render_separator=" "
	done
}

# Purpose: Validate, split, and quote a literal CLI token string.
# Usage: zxfer_quote_cli_tokens STRING [LABEL]; publishes
# g_zxfer_shell_command_result (empty for blank input), or returns 1 with
# g_zxfer_literal_token_error_result on rejection.
zxfer_quote_cli_tokens() {
	g_zxfer_shell_command_result=""
	zxfer_check_literal_token_string "$1" "${2:-CLI command}" || return 1
	zxfer_split_tokens_into_result "$1"
	[ -n "$g_zxfer_split_tokens_result" ] || return 0
	zxfer_split_begin "$ZXFER_LF"
	# shellcheck disable=SC2086 # One checked token per line.
	set -- $g_zxfer_split_tokens_result
	zxfer_split_end
	zxfer_render_shell_command_from_argv "$@"
}

# Purpose: Strip trailing slashes, leaving empty and all-slash inputs unchanged.
# Usage: zxfer_strip_trailing_slashes PATH; publishes
# g_zxfer_stripped_path_result.
zxfer_strip_trailing_slashes() {
	g_zxfer_stripped_path_result=$1
	case $1 in
	*[!/]*)
		while [ "${g_zxfer_stripped_path_result%/}" != "$g_zxfer_stripped_path_result" ]; do
			g_zxfer_stripped_path_result=${g_zxfer_stripped_path_result%/}
		done
		;;
	esac
}
