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
# DEPENDENCY RESOLUTION / SECURE PATH
################################################################################

# Module contract:
# owns globals: g_zxfer_secure_path; the g_cmd_* helper and compression
#   commands with their *_safe renderings; the lookup results
#   g_zxfer_computed_secure_path, g_zxfer_tool_path_result,
#   g_zxfer_normalized_tool_path, g_zxfer_required_tool_result and
#   g_zxfer_resolved_cli_command_result.
# reads globals: ZXFER_SECURE_PATH, ZXFER_SECURE_PATH_APPEND and
#   g_option_z_compress.
# mutates caches: none.
# returns via stdout: none.
# Helper lookup never forks: zxfer_find_tool_in_path walks the PATH list.

# Directories considered safe for PATH lookups. Administrators may override the
# entire list via ZXFER_SECURE_PATH or append additional trusted directories via
# ZXFER_SECURE_PATH_APPEND.
ZXFER_DEFAULT_SECURE_PATH="/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"
ZXFER_INVALID_SECURE_PATH_MESSAGE="Refusing to use ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND because every secure PATH entry must be a single-line absolute path without control whitespace."

# Purpose: Stop startup with a dependency failure for an unusable secure PATH.
# Usage: zxfer_refresh_secure_path_state || zxfer_reject_invalid_secure_path_configuration
zxfer_reject_invalid_secure_path_configuration() {
	zxfer_set_failure_context_if_empty dependency "secure PATH validation" \
		"$ZXFER_INVALID_SECURE_PATH_MESSAGE"
	zxfer_throw_error "$ZXFER_INVALID_SECURE_PATH_MESSAGE"
}

# Purpose: Build the secure PATH from ZXFER_SECURE_PATH (or the default) plus
# ZXFER_SECURE_PATH_APPEND, keeping absolute entries only.
# Usage: zxfer_compute_secure_path; publishes g_zxfer_computed_secure_path,
# or returns 1 with it empty when the value holds a tab, CR or LF.
zxfer_compute_secure_path() {
	g_zxfer_computed_secure_path=""
	l_candidate=${ZXFER_SECURE_PATH:-$ZXFER_DEFAULT_SECURE_PATH}
	if [ -n "${ZXFER_SECURE_PATH_APPEND:-}" ]; then
		l_candidate=${l_candidate:+$l_candidate:}$ZXFER_SECURE_PATH_APPEND
	fi
	zxfer_value_is_single_line "$l_candidate" || return 1

	l_clean=""
	zxfer_split_begin :
	for l_entry in $l_candidate; do
		# Empty, "." and other relative entries never reach PATH.
		case $l_entry in
		/*) l_clean=${l_clean:+$l_clean:}$l_entry ;;
		esac
	done
	zxfer_split_end
	g_zxfer_computed_secure_path=${l_clean:-$ZXFER_DEFAULT_SECURE_PATH}
}

# Purpose: Recompute g_zxfer_secure_path from the environment.
# Usage: zxfer_refresh_secure_path_state || zxfer_reject_invalid_secure_path_configuration
zxfer_refresh_secure_path_state() {
	zxfer_compute_secure_path || return
	g_zxfer_secure_path=$g_zxfer_computed_secure_path
}

# Purpose: Narrow the live PATH to the secure PATH so a bare command name can
# never resolve outside it.
# Usage: zxfer_apply_secure_path, once startup no longer needs helpers such as
# mktemp from outside a narrow ZXFER_SECURE_PATH. Throws when the secure PATH
# is empty, because an empty PATH makes bash and dash search the cwd.
zxfer_apply_secure_path() {
	[ -n "${g_zxfer_secure_path:-}" ] ||
		zxfer_reject_invalid_secure_path_configuration
	PATH=$g_zxfer_secure_path
	export PATH
}

# Purpose: Find an executable regular file on a colon-separated directory list
# without forking, as command -v does for a utility.
# Usage: zxfer_find_tool_in_path TOOL PATHLIST; publishes the match in
# g_zxfer_tool_path_result or returns 1. Relative list entries are skipped, and
# a TOOL containing / is checked as given.
zxfer_find_tool_in_path() {
	g_zxfer_tool_path_result=""
	case $1 in
	'') return 1 ;;
	*/*)
		[ -f "$1" ] && [ -x "$1" ] || return 1
		g_zxfer_tool_path_result=$1
		return 0
		;;
	esac

	l_tool_path_rest=$2:
	while [ -n "$l_tool_path_rest" ]; do
		l_tool_path_dir=${l_tool_path_rest%%:*}
		l_tool_path_rest=${l_tool_path_rest#*:}
		# Drop one trailing slash so "/usr/bin/" yields "/usr/bin/TOOL".
		case $l_tool_path_dir in
		/*) l_tool_path_dir=${l_tool_path_dir%/} ;;
		*) continue ;;
		esac
		if [ -f "$l_tool_path_dir/$1" ] && [ -x "$l_tool_path_dir/$1" ]; then
			g_zxfer_tool_path_result=$l_tool_path_dir/$1
			return 0
		fi
	done
	return 1
}

# Purpose: Undo the shell quoting that some /bin/sh builds (OmniOS) add to
# command -v output, and drop trailing newlines as a $(...) capture would.
# Usage: zxfer_normalize_resolved_tool_path PATH; publishes
# g_zxfer_normalized_tool_path and prints nothing.
zxfer_normalize_resolved_tool_path() {
	g_zxfer_normalized_tool_path=$1
	case $1 in
	\'/*\')
		l_unquoted_path=${1#\'}
		l_unquoted_path=${l_unquoted_path%\'}
		case $l_unquoted_path in
		*\'*) ;;
		*) g_zxfer_normalized_tool_path=$l_unquoted_path ;;
		esac
		;;
	\"/*\")
		l_unquoted_path=${1#\"}
		l_unquoted_path=${l_unquoted_path%\"}
		case $l_unquoted_path in
		*\"*) ;;
		*) g_zxfer_normalized_tool_path=$l_unquoted_path ;;
		esac
		;;
	esac

	while :; do
		case $g_zxfer_normalized_tool_path in
		*"$ZXFER_LF") g_zxfer_normalized_tool_path=${g_zxfer_normalized_tool_path%"$ZXFER_LF"} ;;
		*) break ;;
		esac
	done
}

# Purpose: Check that a resolved helper path is one absolute line.
# Usage: zxfer_validate_resolved_tool_path PATH LABEL [SCOPE]; publishes the
# normalized path in g_zxfer_required_tool_result, or returns 1 with the
# operator message there.
zxfer_validate_resolved_tool_path() {
	zxfer_normalize_resolved_tool_path "$1"
	l_validated_tool_path=$g_zxfer_normalized_tool_path
	l_validated_tool_problem=""
	if ! zxfer_value_is_single_line "$l_validated_tool_path"; then
		l_validated_tool_problem="a single-line absolute path without control whitespace"
	else
		case $l_validated_tool_path in
		/*) ;;
		*) l_validated_tool_problem="an absolute path" ;;
		esac
	fi

	if [ -z "$l_validated_tool_problem" ]; then
		g_zxfer_required_tool_result=$l_validated_tool_path
		return 0
	fi
	g_zxfer_required_tool_result="Required dependency \"$2\"${3:+ on $3} resolved to \"$l_validated_tool_path\", but zxfer requires $l_validated_tool_problem."
	return 1
}

# Purpose: Resolve a required helper to an absolute path on the secure PATH.
# Usage: zxfer_find_required_tool TOOL [LABEL]; publishes the path in
# g_zxfer_required_tool_result, or returns 1 with the operator message there.
# Shell functions and aliases never count.
zxfer_find_required_tool() {
	if zxfer_find_tool_in_path "$1" "${g_zxfer_secure_path:-$ZXFER_DEFAULT_SECURE_PATH}"; then
		zxfer_validate_resolved_tool_path "$g_zxfer_tool_path_result" "${2:-$1}"
		return
	fi
	g_zxfer_required_tool_result="Required dependency \"${2:-$1}\" not found in secure PATH ($g_zxfer_secure_path). Set ZXFER_SECURE_PATH or install the binary."
	return 1
}

# Purpose: Resolve a required helper or stop the run with a dependency failure.
# Usage: zxfer_require_tool TOOL [LABEL], then read the absolute path from
# g_zxfer_required_tool_result.
zxfer_require_tool() {
	zxfer_find_required_tool "$@" ||
		zxfer_throw_dependency_error "$g_zxfer_required_tool_result"
}

# Purpose: Reset the endpoint-safe codecs (the origin compressor and the
# target decompressor) to the local ones.
# Usage: Called before the remote roles replace the command they run.
zxfer_reset_endpoint_compression_commands() {
	g_origin_cmd_compress_safe=$g_cmd_compress_safe
	g_target_cmd_decompress_safe=$g_cmd_decompress_safe
}

# Purpose: Resolve a CLI command's first token, on the local secure PATH or on
# a remote host, and requote the command around the resolved path.
# Usage: zxfer_resolve_cli_command_safe HOST_SPEC STRING [LABEL]
# [PROFILE_SIDE]; an empty HOST_SPEC resolves locally. Publishes the command,
# each token single-quoted, in g_zxfer_resolved_cli_command_result, or
# returns 1 with the operator message there.
zxfer_resolve_cli_command_safe() {
	l_cli_label=${3:-command}
	if ! zxfer_check_literal_token_string "$2" "$l_cli_label"; then
		g_zxfer_resolved_cli_command_result=$g_zxfer_literal_token_error_result
		return 1
	fi
	zxfer_split_tokens_into_result "$2"
	l_cli_tokens=$g_zxfer_split_tokens_result
	if [ -z "$l_cli_tokens" ]; then
		g_zxfer_resolved_cli_command_result="Required dependency \"$l_cli_label\" must not be empty or whitespace-only."
		return 1
	fi
	if [ -n "$1" ]; then
		zxfer_resolve_remote_required_tool "$1" "${l_cli_tokens%%"$ZXFER_LF"*}" \
			"$l_cli_label" "${4:-}"
	else
		zxfer_find_required_tool "${l_cli_tokens%%"$ZXFER_LF"*}" "$l_cli_label"
	fi || {
		g_zxfer_resolved_cli_command_result=$g_zxfer_required_tool_result
		return 1
	}

	zxfer_split_begin "$ZXFER_LF"
	# shellcheck disable=SC2086 # One checked token per line.
	set -- $l_cli_tokens
	zxfer_split_end
	shift
	zxfer_render_shell_command_from_argv "$g_zxfer_required_tool_result" "$@"
	g_zxfer_resolved_cli_command_result=$g_zxfer_shell_command_result
}

# Purpose: Drop every inherited helper command and the secure PATH, and set the
# default compression commands.
# Usage: Called first by zxfer_reset_session_state: later resets copy g_cmd_zfs
# and probe g_cmd_ssh, so neither may keep an inherited value.
zxfer_reset_dependency_state() {
	g_zxfer_secure_path=""
	g_cmd_awk=""
	g_cmd_cat=""
	g_cmd_parallel=""
	g_cmd_ps=""
	g_cmd_ssh=""
	g_cmd_zfs=""
	g_cmd_compress="zstd -3"
	g_cmd_decompress="zstd -d"
	g_cmd_compress_safe=""
	g_cmd_decompress_safe=""
	g_origin_cmd_compress_safe=""
	g_target_cmd_decompress_safe=""
}

# Purpose: Point g_cmd_awk at the awk on the built-in secure PATH, so the EXIT
# trap can render an early failure without running an inherited command.
# Usage: Called by zxfer_session_initialize just before the traps go in.
zxfer_initialize_dependency_reporting_defaults() {
	g_cmd_awk='awk'
	if zxfer_find_tool_in_path awk "$ZXFER_DEFAULT_SECURE_PATH"; then
		g_cmd_awk=$g_zxfer_tool_path_result
	fi
}

# Purpose: Validate -z/-Z and resolve the local compression and decompression
# commands into g_cmd_compress_safe and g_cmd_decompress_safe.
# Usage: Called once after option parsing. Without -z (which -Z implies) both
# stay empty, since every consumer is gated on -z.
zxfer_refresh_compression_commands() {
	g_cmd_compress_safe=""
	g_cmd_decompress_safe=""
	[ "$g_option_z_compress" -eq 1 ] || return 0

	zxfer_check_literal_token_string "$g_cmd_compress" "Compression command (-Z)" ||
		zxfer_throw_usage_error "$g_zxfer_literal_token_error_result" 2
	zxfer_split_tokens_into_result "$g_cmd_compress"
	[ -n "$g_zxfer_split_tokens_result" ] ||
		zxfer_throw_usage_error "Compression command (-Z) cannot be empty." 2
	zxfer_check_literal_token_string "$g_cmd_decompress" "Decompression command" ||
		zxfer_throw_error "$g_zxfer_literal_token_error_result"
	zxfer_split_tokens_into_result "$g_cmd_decompress"
	[ -n "$g_zxfer_split_tokens_result" ] ||
		zxfer_throw_error "Compression requested but decompression command missing."
	zxfer_resolve_cli_command_safe "" "$g_cmd_compress" "compression command" ||
		zxfer_throw_dependency_error "$g_zxfer_resolved_cli_command_result"
	g_cmd_compress_safe=$g_zxfer_resolved_cli_command_result
	zxfer_resolve_cli_command_safe "" "$g_cmd_decompress" "decompression command" ||
		zxfer_throw_dependency_error "$g_zxfer_resolved_cli_command_result"
	g_cmd_decompress_safe=$g_zxfer_resolved_cli_command_result
}

# Purpose: Resolve the helpers every run needs on the secure PATH.
# Usage: Called by zxfer_init_session_environment after
# zxfer_refresh_secure_path_state; throws when awk, zfs or ps is missing.
zxfer_init_dependency_tool_defaults() {
	zxfer_require_tool awk
	g_cmd_awk=$g_zxfer_required_tool_result
	zxfer_require_tool zfs
	g_cmd_zfs=$g_zxfer_required_tool_result
	# parallel is optional, but a parallel that is present must validate.
	g_cmd_parallel=""
	if zxfer_find_tool_in_path parallel "$g_zxfer_secure_path"; then
		zxfer_require_tool parallel
		g_cmd_parallel=$g_zxfer_required_tool_result
	fi
	zxfer_require_tool ps
	g_cmd_ps=$g_zxfer_required_tool_result
}
