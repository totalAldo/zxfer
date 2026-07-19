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
# owns globals: g_zxfer_profile_command_render_calls.
# reads globals: g_option_V_very_verbose only to retain the established profile
# counter activation rule.
# mutates caches: command-render count only.
# returns via stdout: validated token streams and quoted command strings.

# Purpose: Reset quoting-owned diagnostic state for a new session.
# Usage: Called by the session composition root before command rendering starts.
zxfer_reset_quoting_state() {
	g_zxfer_profile_command_render_calls=0
}

# Purpose: Record one rendered command without coupling quoting to profiling.
# Usage: Called by the two renderers whose invocations are part of the existing
# `-V` performance contract.
zxfer_note_command_render() {
	[ "${g_option_V_very_verbose:-0}" -eq 1 ] || return 0
	g_zxfer_profile_command_render_calls=$((${g_zxfer_profile_command_render_calls:-0} + 1))
}

# Purpose: Escape a value for reinsertion into a single-quoted shell string.
# Usage: Called before values are embedded in rendered shell commands or
# remote helper payloads.
zxfer_escape_for_single_quotes() {
	case $1 in
	*\'*) ;;
	*)
		printf '%s' "$1"
		return 0
		;;
	esac

	l_escape_rest=$1
	l_escape_out=""
	while :; do
		case $l_escape_rest in
		*\'*)
			l_escape_out="$l_escape_out${l_escape_rest%%\'*}'\\''"
			l_escape_rest=${l_escape_rest#*\'}
			;;
		*)
			l_escape_out="$l_escape_out$l_escape_rest"
			break
			;;
		esac
	done
	printf '%s' "$l_escape_out"
}

# Purpose: Split a literal whitespace-delimited string into one token per line.
# Usage: Called when zxfer must preserve argument boundaries without invoking a
# shell parser. Callers must not use this as a quoting-aware parser.
zxfer_split_tokens_on_whitespace() {
	l_input=$1
	if [ "$l_input" = "" ]; then
		return
	fi

	case $l_input in
	*\;* | *\|* | *\&*)
		l_split_rest=$l_input
		l_normalized_input=""
		while :; do
			l_split_head=${l_split_rest%%[;\|\&]*}
			if [ "$l_split_head" = "$l_split_rest" ]; then
				l_normalized_input="$l_normalized_input$l_split_rest"
				break
			fi
			l_split_rest=${l_split_rest#"$l_split_head"}
			l_split_char=${l_split_rest%"${l_split_rest#?}"}
			l_split_rest=${l_split_rest#?}
			l_normalized_input="$l_normalized_input$l_split_head$l_split_char "
		done
		;;
	*)
		l_normalized_input=$l_input
		;;
	esac

	case $- in
	*f*)
		l_split_restore_glob=0
		;;
	*)
		l_split_restore_glob=1
		set -f
		;;
	esac
	if [ "${IFS+set}" = "set" ]; then
		l_split_saved_ifs_set=1
		l_split_saved_ifs=$IFS
	else
		l_split_saved_ifs_set=0
		l_split_saved_ifs=""
	fi
	unset IFS
	# shellcheck disable=SC2086  # Intentional literal whitespace splitting.
	set -- $l_normalized_input
	if [ "$l_split_saved_ifs_set" -eq 1 ]; then
		IFS=$l_split_saved_ifs
	fi
	if [ "$l_split_restore_glob" -eq 1 ]; then
		set +f
	fi

	for l_split_token in "$@"; do
		printf '%s\n' "$l_split_token"
	done
}

# Purpose: Reject token strings that require unsupported shell parsing.
# Usage: Called before string interfaces are split into argv-like streams.
zxfer_validate_literal_token_string() {
	l_input=$1
	l_label=${2:-command}

	case "$l_input" in
	*\\* | *\"* | *\'*)
		printf '%s\n' "$l_label must use literal whitespace-delimited tokens only; shell quotes and backslash escapes are not supported."
		return 1
		;;
	esac

	printf '%s\n' "$l_input"
}

# Purpose: Validate and split a CLI token string.
# Usage: Called before dependency and host specifications are normalized.
zxfer_split_cli_tokens() {
	l_cli_string=$1
	l_label=${2:-CLI command}

	if ! l_cli_string=$(zxfer_validate_literal_token_string "$l_cli_string" "$l_label"); then
		printf '%s\n' "$l_cli_string"
		return 1
	fi

	zxfer_split_tokens_on_whitespace "$l_cli_string"
}

# Purpose: Quote a newline-delimited token stream for shell rendering.
# Usage: Called after tokens have already been validated and separated.
zxfer_quote_token_stream() {
	l_tokens=$1
	if [ "$l_tokens" = "" ]; then
		return
	fi

	zxfer_note_command_render
	l_output=""
	while IFS= read -r l_token || [ -n "$l_token" ]; do
		[ "$l_token" = "" ] && continue
		l_safe_token=$(zxfer_escape_for_single_quotes "$l_token")
		if [ "$l_output" = "" ]; then
			l_output="'$l_safe_token'"
		else
			l_output="$l_output '$l_safe_token'"
		fi
	done <<EOF
$l_tokens
EOF
	printf '%s' "$l_output"
}

# Purpose: Build a safely quoted shell command from positional arguments.
# Usage: Called only for rendered-shell APIs and operator display, never as a
# substitute for the direct-argv execution API.
zxfer_build_shell_command_from_argv() {
	zxfer_note_command_render
	l_output=""
	for l_arg in "$@"; do
		l_safe_arg=$(zxfer_escape_for_single_quotes "$l_arg")
		if [ "$l_output" = "" ]; then
			l_output="'$l_safe_arg'"
		else
			l_output="$l_output '$l_safe_arg'"
		fi
	done

	printf '%s' "$l_output"
}

# Purpose: Validate, split, and quote a literal CLI token string.
# Usage: Called when a documented string interface must be rendered safely.
zxfer_quote_cli_tokens() {
	l_cli_string=$1
	l_label=${2:-CLI command}
	if [ "$l_cli_string" = "" ]; then
		return
	fi

	if ! l_quote_cli_tokens_tokens=$(zxfer_split_cli_tokens "$l_cli_string" "$l_label"); then
		printf '%s\n' "$l_quote_cli_tokens_tokens"
		return 1
	fi
	if [ "$l_quote_cli_tokens_tokens" = "" ]; then
		return
	fi

	zxfer_quote_token_stream "$l_quote_cli_tokens_tokens"
}

# Purpose: Strip trailing slashes without changing all-slash inputs.
# Usage: Called before dataset-like arguments are validated and consumed.
zxfer_strip_trailing_slashes() {
	l_path=$1

	case "$l_path" in
	*[!/]*) ;;
	*)
		printf '%s\n' "$l_path"
		return
		;;
	esac

	while [ "${l_path%/}" != "$l_path" ]; do
		l_path=${l_path%/}
	done

	printf '%s\n' "$l_path"
}
