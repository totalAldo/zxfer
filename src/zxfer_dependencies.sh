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

	OLDIFS=$IFS
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
	IFS=$OLDIFS

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

# Purpose: Merge the path allowlists while preserving zxfer's precedence rules.
# Usage: Called during secure-PATH bootstrap and local dependency resolution
# when multiple configuration sources contribute to one effective value.
zxfer_merge_path_allowlists() {
	l_primary=$1
	l_secondary=$2

	OLDIFS=$IFS
	IFS=":"
	l_merged=""
	for l_entry in $l_primary $l_secondary; do
		[ -n "$l_entry" ] || continue
		case ":$l_merged:" in
		*:"$l_entry":*)
			continue
			;;
		esac
		if [ "$l_merged" = "" ]; then
			l_merged=$l_entry
		else
			l_merged=$l_merged:$l_entry
		fi
	done
	IFS=$OLDIFS

	printf '%s\n' "$l_merged"
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

# Purpose: Assign the required tool into the shared runtime variable that owns
# it.
# Usage: Called during secure-PATH bootstrap and local dependency resolution
# after a validated lookup succeeds and downstream helpers should reuse the
# stored result.
zxfer_assign_required_tool() {
	l_var_name=$1
	l_tool=$2
	l_label=${3:-$l_tool}

	if ! l_resolved_path=$(zxfer_find_required_tool "$l_tool" "$l_label"); then
		g_zxfer_failure_class=dependency
		zxfer_throw_error "$l_resolved_path"
	fi

	eval "$l_var_name=\$l_resolved_path"
}

# Purpose: Rebuild a CLI command string around a validated absolute helper path
# for its head token.
# Usage: Called during secure-PATH bootstrap and local dependency resolution
# after the command head is resolved so later rendering keeps the caller's
# remaining arguments intact.
zxfer_requote_cli_command_with_resolved_head() {
	l_cli_string=$1
	l_resolved_head=$2
	l_cli_tokens=$(zxfer_split_cli_tokens "$l_cli_string")
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
	l_cli_tokens=$(zxfer_split_cli_tokens "$l_cli_string")
	l_cli_head=$(printf '%s\n' "$l_cli_tokens" | sed -n '1p')
	if [ -z "$l_cli_head" ]; then
		printf '%s\n' "Required dependency \"$l_label\" must not be empty or whitespace-only."
		return 1
	fi

	if ! l_resolved_head=$(zxfer_find_required_tool "$l_cli_head" "$l_label"); then
		printf '%s\n' "$l_resolved_head"
		return 1
	fi

	zxfer_requote_cli_command_with_resolved_head "$l_cli_string" "$l_resolved_head"
}

# Purpose: Initialize the dependency defaults before later helpers depend on
# it.
# Usage: Called during secure-PATH bootstrap and local dependency resolution
# during bootstrap so downstream code sees consistent defaults and runtime
# state.
zxfer_initialize_dependency_defaults() {
	zxfer_refresh_secure_path_state

	if [ -z "${g_cmd_awk:-}" ]; then
		l_search_path=${g_zxfer_dependency_path:-$g_zxfer_secure_path}
		[ -n "$l_search_path" ] || l_search_path=$ZXFER_DEFAULT_SECURE_PATH
		g_cmd_awk=$(PATH=$l_search_path command -v awk 2>/dev/null || :)
		if [ -z "$g_cmd_awk" ]; then
			g_cmd_awk='awk'
		fi
	fi
}
