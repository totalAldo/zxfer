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
# owns globals: secure-PATH defaults and local helper resolutions initialized here.
# reads globals: ZXFER_SECURE_PATH*, PATH, and g_cmd_awk fallback needs.
# mutates caches: none.
# returns via stdout: secure PATH strings and validated absolute helper paths.

# Directories considered safe for PATH lookups. Administrators may override the
# entire list via ZXFER_SECURE_PATH or append additional trusted directories via
# ZXFER_SECURE_PATH_APPEND.
ZXFER_DEFAULT_SECURE_PATH="/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

# Purpose: Compute the secure path from the active configuration and runtime
# state.
# Usage: Called during secure-PATH bootstrap and local dependency resolution
# when later helpers need a derived value without duplicating the calculation.
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

	case $- in
	*f*)
		l_secure_path_restore_glob=0
		;;
	*)
		l_secure_path_restore_glob=1
		set -f
		;;
	esac
	if [ "${IFS+set}" = "set" ]; then
		l_secure_path_saved_ifs_set=1
		l_secure_path_saved_ifs=$IFS
	else
		l_secure_path_saved_ifs_set=0
		l_secure_path_saved_ifs=""
	fi
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
	if [ "$l_secure_path_saved_ifs_set" -eq 1 ]; then
		IFS=$l_secure_path_saved_ifs
	else
		unset IFS
	fi
	if [ "$l_secure_path_restore_glob" -eq 1 ]; then
		set +f
	fi

	if [ "$l_clean" = "" ]; then
		l_clean=$ZXFER_DEFAULT_SECURE_PATH
	fi

	printf '%s\n' "$l_clean"
}

# Purpose: Return the effective dependency path in the form expected by later
# helpers.
# Usage: Called during secure-PATH bootstrap and local dependency resolution
# when sibling helpers need the same lookup without duplicating module logic.
zxfer_get_effective_dependency_path() {
	if [ -n "${ZXFER_SECURE_PATH:-}" ] || [ -n "${ZXFER_SECURE_PATH_APPEND:-}" ]; then
		zxfer_compute_secure_path
		return
	fi

	if [ -n "${g_zxfer_dependency_path:-}" ]; then
		printf '%s\n' "$g_zxfer_dependency_path"
		return
	fi
	if [ -n "${g_zxfer_secure_path:-}" ]; then
		printf '%s\n' "$g_zxfer_secure_path"
		return
	fi

	printf '%s\n' "$ZXFER_DEFAULT_SECURE_PATH"
}

# Purpose: Refresh the secure path state from the current configuration and
# runtime state.
# Usage: Called during secure-PATH bootstrap and local dependency resolution
# after inputs change and downstream helpers need the derived value rebuilt.
zxfer_refresh_secure_path_state() {
	g_zxfer_secure_path=$(zxfer_compute_secure_path)
	g_zxfer_dependency_path=$g_zxfer_secure_path
	g_zxfer_runtime_path=$g_zxfer_secure_path
}

# Purpose: Apply the secure path through the controlled helper path owned by
# this module.
# Usage: Called during secure-PATH bootstrap and local dependency resolution
# once planning is complete and zxfer is ready to mutate live state.
zxfer_apply_secure_path() {
	zxfer_refresh_secure_path_state
	# Keep the live runtime PATH equal to the configured secure allowlist so
	# later bare helper lookups cannot escape an explicit ZXFER_SECURE_PATH.
	PATH=$g_zxfer_runtime_path
	export PATH
}

# Purpose: Normalize the resolved tool path into the stable form used across
# zxfer.
# Usage: Called during secure-PATH bootstrap and local dependency resolution
# before comparison, caching, or reporting depends on exact formatting.
zxfer_normalize_resolved_tool_path() {
	l_path=$1

	# Some /bin/sh implementations (including OmniOS) shell-quote absolute
	# command -v results when helper paths contain metacharacters.
	case "$l_path" in
	\'/*\')
		l_unquoted_path=${l_path#\'}
		l_unquoted_path=${l_unquoted_path%\'}
		case "$l_unquoted_path" in
		*"'"*) ;;
		*)
			printf '%s\n' "$l_unquoted_path"
			return 0
			;;
		esac
		;;
	\"/*\")
		l_unquoted_path=${l_path#\"}
		l_unquoted_path=${l_unquoted_path%\"}
		case "$l_unquoted_path" in
		*'"'*) ;;
		*)
			printf '%s\n' "$l_unquoted_path"
			return 0
			;;
		esac
		;;
	esac

	printf '%s\n' "$l_path"
}

# Purpose: Validate the resolved tool path before zxfer relies on it.
# Usage: Called during secure-PATH bootstrap and local dependency resolution to
# fail closed on malformed, unsafe, or stale input.
zxfer_validate_resolved_tool_path() {
	l_path=$1
	l_label=$2
	l_scope=${3:-}

	l_path=$(zxfer_normalize_resolved_tool_path "$l_path")
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

# Purpose: Find the required tool in the tracked state owned by this module.
# Usage: Called during secure-PATH bootstrap and local dependency resolution
# when later helpers need an existing record instead of rebuilding one.
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

# Purpose: Publish one resolved command through the dependency owner.
# Usage: The explicit mapping keeps every supported helper visible to static
# ownership checks and avoids indirect assignment.
zxfer_set_dependency_command() {
	l_var_name=$1
	l_command_value=${2:-}

	case "$l_var_name" in
	g_cmd_awk) g_cmd_awk=$l_command_value ;;
	g_cmd_cat) g_cmd_cat=$l_command_value ;;
	g_cmd_compress) g_cmd_compress=$l_command_value ;;
	g_cmd_compress_safe) g_cmd_compress_safe=$l_command_value ;;
	g_cmd_decompress) g_cmd_decompress=$l_command_value ;;
	g_cmd_decompress_safe) g_cmd_decompress_safe=$l_command_value ;;
	g_cmd_parallel) g_cmd_parallel=$l_command_value ;;
	g_cmd_ps) g_cmd_ps=$l_command_value ;;
	g_cmd_ssh) g_cmd_ssh=$l_command_value ;;
	g_cmd_zfs) g_cmd_zfs=$l_command_value ;;
	*) return 2 ;;
	esac
}

# Purpose: Publish one role-specific safe compression command.
# Usage: Endpoint initialization passes validated `origin|target` and
# `compress|decompress` selectors through this owner operation.
zxfer_set_endpoint_compression_command() {
	l_endpoint_role=$1
	l_endpoint_codec=$2
	l_endpoint_command=${3:-}

	case "$l_endpoint_role:$l_endpoint_codec" in
	origin:compress) g_origin_cmd_compress_safe=$l_endpoint_command ;;
	origin:decompress) g_origin_cmd_decompress_safe=$l_endpoint_command ;;
	target:compress) g_target_cmd_compress_safe=$l_endpoint_command ;;
	target:decompress) g_target_cmd_decompress_safe=$l_endpoint_command ;;
	*) return 2 ;;
	esac
}

# Purpose: Reset endpoint-safe commands to the resolved local defaults.
# Usage: Called before remote roles selectively replace their command.
zxfer_reset_endpoint_compression_commands() {
	zxfer_set_endpoint_compression_command origin compress "$g_cmd_compress_safe"
	zxfer_set_endpoint_compression_command origin decompress "$g_cmd_decompress_safe"
	zxfer_set_endpoint_compression_command target compress "$g_cmd_compress_safe"
	zxfer_set_endpoint_compression_command target decompress "$g_cmd_decompress_safe"
}

# Purpose: Resolve and assign one required dependency command.
# Usage: Called during secure-PATH bootstrap after callers select a supported
# dependency-owned command slot.
zxfer_assign_required_tool() {
	l_var_name=$1
	l_tool=$2
	l_label=${3:-$l_tool}

	if ! zxfer_set_dependency_command "$l_var_name" ""; then
		zxfer_set_failure_class dependency
		zxfer_throw_error "Invalid internal dependency assignment target."
	fi

	if ! l_resolved_path=$(zxfer_find_required_tool "$l_tool" "$l_label"); then
		zxfer_set_failure_class dependency
		zxfer_throw_error "$l_resolved_path"
	fi

	zxfer_set_dependency_command "$l_var_name" "$l_resolved_path"
}

# Purpose: Rebuild a CLI command string around a validated absolute helper path
# for its head token.
# Usage: Called during secure-PATH bootstrap and local dependency resolution
# after the command head is resolved so later rendering keeps the caller's
# remaining arguments intact.
zxfer_requote_cli_command_with_resolved_head() {
	l_cli_string=$1
	l_resolved_head=$2
	l_label=${3:-CLI command}
	if ! l_cli_tokens=$(zxfer_split_cli_tokens "$l_cli_string" "$l_label"); then
		printf '%s\n' "$l_cli_tokens"
		return 1
	fi
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
	zxfer_quote_token_stream "$l_output_tokens"
}

# Purpose: Resolve the effective local CLI command safe that zxfer should use.
# Usage: Called during secure-PATH bootstrap and local dependency resolution
# after configuration, cache state, or remote state can change the final
# choice.
zxfer_resolve_local_cli_command_safe() {
	l_cli_string=$1
	l_label=${2:-command}
	if ! l_cli_tokens=$(zxfer_split_cli_tokens "$l_cli_string" "$l_label"); then
		printf '%s\n' "$l_cli_tokens"
		return 1
	fi
	l_cli_head=$(printf '%s\n' "$l_cli_tokens" | sed -n '1p')
	if [ -z "$l_cli_head" ]; then
		printf '%s\n' "Required dependency \"$l_label\" must not be empty or whitespace-only."
		return 1
	fi

	if ! l_resolved_head=$(zxfer_find_required_tool "$l_cli_head" "$l_label"); then
		printf '%s\n' "$l_resolved_head"
		return 1
	fi

	zxfer_requote_cli_command_with_resolved_head "$l_cli_string" "$l_resolved_head" "$l_label"
}

# Purpose: Initialize the dependency defaults before later helpers depend on
# it.
# Usage: Called during secure-PATH bootstrap and local dependency resolution
# during bootstrap so downstream code sees consistent defaults and runtime
# state.
zxfer_initialize_dependency_defaults() {
	zxfer_refresh_secure_path_state

	# This bootstrap value is reachable from the EXIT trap's failure renderer
	# before the full dependency owner reset runs. Never preserve an inherited
	# internal command global across that boundary.
	l_search_path=${g_zxfer_dependency_path:-$g_zxfer_secure_path}
	[ -n "$l_search_path" ] || l_search_path=$ZXFER_DEFAULT_SECURE_PATH
	g_cmd_awk=$(PATH=$l_search_path command -v awk 2>/dev/null || :)
	if [ -z "$g_cmd_awk" ]; then
		g_cmd_awk='awk'
	fi
}

# Purpose: Refresh the validated compression and decompression command variants
# derived from the parsed CLI configuration and resolved dependency paths.
# Usage: Called after dependency initialization and whenever compression-
# related options change so execution paths reuse one safe command result.
zxfer_refresh_compression_commands() {
	if [ "$g_option_z_compress" -eq 1 ]; then
		if [ "$g_cmd_compress" = "" ]; then
			zxfer_throw_usage_error "Compression command (-Z) cannot be empty." 2
		fi
		if ! l_compress_tokens=$(zxfer_split_cli_tokens "$g_cmd_compress" "Compression command (-Z)"); then
			zxfer_throw_usage_error "$l_compress_tokens" 2
		fi
		if [ "$l_compress_tokens" = "" ]; then
			zxfer_throw_usage_error "Compression command (-Z) cannot be empty." 2
		fi
		if [ "$g_cmd_decompress" = "" ]; then
			zxfer_throw_error "Compression requested but decompression command missing."
		fi
		if ! l_decompress_tokens=$(zxfer_split_cli_tokens "$g_cmd_decompress" "Decompression command"); then
			zxfer_throw_error "$l_decompress_tokens"
		fi
		if [ "$l_decompress_tokens" = "" ]; then
			zxfer_throw_error "Compression requested but decompression command missing."
		fi
		if ! g_cmd_compress_safe=$(zxfer_resolve_local_cli_command_safe "$g_cmd_compress" "compression command"); then
			zxfer_set_failure_class dependency
			zxfer_throw_error "$g_cmd_compress_safe"
		fi
		if ! g_cmd_decompress_safe=$(zxfer_resolve_local_cli_command_safe "$g_cmd_decompress" "decompression command"); then
			zxfer_set_failure_class dependency
			zxfer_throw_error "$g_cmd_decompress_safe"
		fi
		return
	fi

	if ! g_cmd_compress_safe=$(zxfer_quote_cli_tokens "$g_cmd_compress" "Compression command"); then
		zxfer_throw_error "$g_cmd_compress_safe"
	fi
	if ! g_cmd_decompress_safe=$(zxfer_quote_cli_tokens "$g_cmd_decompress" "Decompression command"); then
		zxfer_throw_error "$g_cmd_decompress_safe"
	fi
}

# Purpose: Reset and resolve the local command defaults for a new session.
# Usage: Called by the session composition root after all modules are loaded
# and before the runtime PATH is narrowed to the validated dependency path.
# Side effects: Publishes validated g_cmd_* helper and compression state.
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
		g_cmd_parallel=$(zxfer_validate_resolved_tool_path "$g_cmd_parallel" "parallel") ||
			l_dependency_status=$?
		if [ "$l_dependency_status" -ne 0 ]; then
			zxfer_set_failure_class dependency
			zxfer_throw_error "$g_cmd_parallel" "$l_dependency_status"
		fi
	fi

	# enable compression in ssh options so that remote snapshot lists that
	# contain thousands of snapshots are compressed
	zxfer_assign_required_tool g_cmd_ps ps "ps"
	zxfer_refresh_compression_commands
}
