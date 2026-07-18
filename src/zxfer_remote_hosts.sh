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
# REMOTE CAPABILITY NEGOTIATION / TOOL RESOLUTION
################################################################################

# Module contract:
# owns globals: per-role capability responses, parsed remote OS/tool records,
#   probe capture state, and resolved remote helper selections.
# reads globals: parsed host options, secure dependency PATH, SSH transport
#   command channels, runtime artifact helpers, and profile/reporting state.
# mutates caches: per-run in-memory capability responses only; no transport or
#   cross-process state.
# returns via stdout: remote capability payloads, OS values, and tool paths.

# Purpose: Reset per-run remote capability and resolved-tool state.
# Usage: Called by the session composition root after dependency and transport
# defaults are available but before any live remote negotiation.
# Side effects: Clears capability/probe caches and restores remote ZFS helpers
# to the validated local default until a remote role is resolved.
zxfer_reset_remote_host_state() {
	g_origin_remote_capabilities_host=""
	g_origin_remote_capabilities_cache_identity=""
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_bootstrap_source=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_cache_identity=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_bootstrap_source=""
	g_zxfer_remote_capability_response_result=""
	g_zxfer_remote_capability_os=""
	g_zxfer_remote_capability_zfs_status=""
	g_zxfer_remote_capability_tool_records=""
	g_zxfer_remote_capability_tool_status_result=""
	g_zxfer_remote_capability_tool_path_result=""
	g_zxfer_remote_capability_requested_tools_result=""
	g_zxfer_remote_probe_stdout=""
	g_zxfer_remote_probe_stderr=""
	g_zxfer_remote_probe_capture_read_result=""
	g_zxfer_remote_probe_capture_failed=0
	g_source_operating_system=""
	g_destination_operating_system=""
	g_origin_cmd_zfs=$g_cmd_zfs
	g_target_cmd_zfs=$g_cmd_zfs
}

# Purpose: Publish one endpoint's resolved operating system and ZFS command.
# Usage: Session initialization passes a validated `origin|target` selector;
# callers never assign role-prefixed capability globals directly.
zxfer_publish_endpoint_runtime_context() {
	l_endpoint_role=$1
	l_endpoint_os=${2:-}
	l_endpoint_zfs_command=${3:-}

	case "$l_endpoint_role" in
	origin)
		g_source_operating_system=$l_endpoint_os
		g_origin_cmd_zfs=$l_endpoint_zfs_command
		;;
	target)
		g_destination_operating_system=$l_endpoint_os
		g_target_cmd_zfs=$l_endpoint_zfs_command
		;;
	*)
		return 2
		;;
	esac
}

# Purpose: Reset the remote capability parse state so the next remote-host pass
# starts from a clean state.
# Usage: Called during capability negotiation and remote tool-
# resolution before this module reuses mutable scratch globals or cached
# decisions.
zxfer_reset_remote_capability_parse_state() {
	g_zxfer_remote_capability_os=""
	g_zxfer_remote_capability_zfs_status=""
	g_zxfer_remote_capability_tool_records=""
	g_zxfer_remote_capability_tool_status_result=""
	g_zxfer_remote_capability_tool_path_result=""
	g_zxfer_remote_capability_record_tool_result=""
	g_zxfer_remote_capability_record_status_result=""
	g_zxfer_remote_capability_record_path_result=""
}

# Purpose: Append the remote capability tool record to the module-owned
# accumulator.
# Usage: Called during capability negotiation and remote tool-
# resolution when later helpers need one shared place to extend staged
# or in-memory state.
zxfer_append_remote_capability_tool_record() {
	l_capability_tool=$1
	l_capability_status=$2
	l_capability_path=$3

	[ -n "$l_capability_tool" ] || return 1

	if [ -n "${g_zxfer_remote_capability_tool_records:-}" ]; then
		g_zxfer_remote_capability_tool_records=$g_zxfer_remote_capability_tool_records'
'$l_capability_tool'	'$l_capability_status'	'$l_capability_path
	else
		g_zxfer_remote_capability_tool_records=$l_capability_tool'	'$l_capability_status'	'$l_capability_path
	fi
}

# Purpose: Return the parsed remote capability tool record in the form expected
# by later helpers.
# Usage: Called during capability negotiation and remote tool-
# resolution when sibling helpers need the same lookup without
# duplicating module logic.
zxfer_get_parsed_remote_capability_tool_record() {
	l_capability_tool=$1
	l_tab='	'

	g_zxfer_remote_capability_tool_status_result=""
	g_zxfer_remote_capability_tool_path_result=""
	[ -n "$l_capability_tool" ] || return 1

	while IFS= read -r l_capability_record || [ -n "$l_capability_record" ]; do
		[ -n "$l_capability_record" ] || continue
		case "$l_capability_record" in
		"$l_capability_tool""$l_tab"*)
			l_capability_record_rest=${l_capability_record#"$l_capability_tool""$l_tab"}
			l_capability_record_status=${l_capability_record_rest%%"$l_tab"*}
			if [ "$l_capability_record_status" = "$l_capability_record_rest" ]; then
				return 1
			fi
			l_capability_record_path=${l_capability_record_rest#*"$l_tab"}
			g_zxfer_remote_capability_tool_status_result=$l_capability_record_status
			g_zxfer_remote_capability_tool_path_result=$l_capability_record_path
			return 0
			;;
		esac
	done <<EOF
${g_zxfer_remote_capability_tool_records:-}
EOF

	return 1
}

# Purpose: Check whether the remote capability requested tool is present.
# Usage: Called during capability negotiation and remote tool-
# resolution when later helpers need a boolean answer about the remote
# capability requested tool.
zxfer_remote_capability_requested_tool_is_present() {
	l_tool=$1

	[ -n "$l_tool" ] || return 1
	while IFS= read -r l_existing_tool || [ -n "$l_existing_tool" ]; do
		[ -n "$l_existing_tool" ] || continue
		[ "$l_existing_tool" = "$l_tool" ] && return 0
	done <<EOF
${g_zxfer_remote_capability_requested_tools_result:-}
EOF

	return 1
}

# Purpose: Append the remote capability requested tool to the module-owned
# accumulator.
# Usage: Called during capability negotiation and remote tool-
# resolution when later helpers need one shared place to extend staged
# or in-memory state.
zxfer_append_remote_capability_requested_tool() {
	l_tool=$1

	[ -n "$l_tool" ] || return 0
	if zxfer_remote_capability_requested_tool_is_present "$l_tool"; then
		return 0
	fi

	if [ -n "${g_zxfer_remote_capability_requested_tools_result:-}" ]; then
		g_zxfer_remote_capability_requested_tools_result=$g_zxfer_remote_capability_requested_tools_result'
'$l_tool
	else
		g_zxfer_remote_capability_requested_tools_result=$l_tool
	fi
}

# Purpose: Render the remote capability requested tools as a stable shell-safe
# or operator-facing string.
# Usage: Called during capability negotiation and remote tool-
# resolution when zxfer needs to display or transport the value without
# reparsing it.
zxfer_render_remote_capability_requested_tools() {
	g_zxfer_remote_capability_requested_tools_result=""
	zxfer_append_remote_capability_requested_tool zfs

	while [ $# -gt 0 ]; do
		zxfer_append_remote_capability_requested_tool "$1"
		shift
	done

	printf '%s\n' "$g_zxfer_remote_capability_requested_tools_result"
}

# Purpose: Resolve the effective remote capability requested tools for host
# that zxfer should use.
# Usage: Called during capability negotiation and remote tool-
# resolution after configuration, cache state, or remote state can
# change the final choice.
zxfer_resolve_remote_capability_requested_tools_for_host() {
	l_host_spec=$1
	l_requested_tools=${2:-}

	if [ -n "$l_requested_tools" ]; then
		zxfer_render_remote_capability_requested_tools >/dev/null
		while IFS= read -r l_tool || [ -n "$l_tool" ]; do
			[ -n "$l_tool" ] || continue
			zxfer_append_remote_capability_requested_tool "$l_tool"
		done <<EOF
$l_requested_tools
EOF
		printf '%s\n' "$g_zxfer_remote_capability_requested_tools_result"
		return 0
	fi

	zxfer_get_remote_capability_requested_tools_for_host "$l_host_spec"
}

# Purpose: Return the remote capability requested tools for tool in the form
# expected by later helpers.
# Usage: Called during capability negotiation and remote tool-
# resolution when sibling helpers need the same lookup without
# duplicating module logic.
zxfer_get_remote_capability_requested_tools_for_tool() {
	l_tool=$1

	case "$l_tool" in
	'' | zfs)
		zxfer_render_remote_capability_requested_tools
		;;
	*)
		zxfer_render_remote_capability_requested_tools "$l_tool"
		;;
	esac
}

# Purpose: Extract the remote CLI command head from the serialized input this
# module works with.
# Usage: Called during capability negotiation and remote tool-
# resolution when later helpers need one field or derived fragment
# without reparsing the full payload themselves.
zxfer_extract_remote_cli_command_head() {
	l_cli_string=$1
	l_label=${2:-CLI command}
	if ! l_cli_tokens=$(zxfer_split_cli_tokens "$l_cli_string" "$l_label"); then
		printf '%s\n' "$l_cli_tokens"
		return 1
	fi
	l_cli_head=$(printf '%s\n' "$l_cli_tokens" | sed -n '1p')
	[ -n "$l_cli_head" ] || return 1
	printf '%s\n' "$l_cli_head"
}

# Purpose: Decide whether the clean recursive no-op proof can defer origin
# parallel resolution.
# Usage: Called while building the active remote capability scope. This mirrors
# the proof eligibility checks in snapshot discovery because this module is
# sourced earlier and cannot call that helper directly.
zxfer_remote_capability_origin_can_defer_parallel_for_fast_noop_proof() {
	[ "${g_option_O_origin_host:-}" != "" ] || return 1
	[ "${g_option_T_target_host:-}" = "" ] || return 1
	[ "${g_option_R_recursive:-}" != "" ] || return 1
	[ "${g_option_s_make_snapshot:-0}" -eq 0 ] || return 1
	[ "${g_option_m_migrate:-0}" -eq 0 ] || return 1
	[ "${g_option_P_transfer_property:-0}" -eq 0 ] || return 1
	[ -z "${g_option_o_override_property:-}" ] || return 1
	[ "${g_option_e_restore_property_mode:-0}" -eq 0 ] || return 1
	[ "${g_option_k_backup_property_mode:-0}" -eq 0 ] || return 1

	return 0
}

# Purpose: Decide whether origin capability preloading should include parallel.
# Usage: Called while building the active remote capability scope. Changed-
# source discovery still honors `-j`, but the fast recursive no-op proof uses
# one recursive source stream, so clean no-op startup can defer parallel until a
# fallback path actually needs it.
zxfer_remote_capability_origin_should_preload_parallel() {
	[ "${g_option_j_jobs:-1}" -gt 1 ] || return 1
	if zxfer_remote_capability_origin_can_defer_parallel_for_fast_noop_proof; then
		return 1
	fi
	return 0
}

# Purpose: Return the remote capability requested tools for host in the form
# expected by later helpers.
# Usage: Called during capability negotiation and remote tool-
# resolution when sibling helpers need the same lookup without
# duplicating module logic.
zxfer_get_remote_capability_requested_tools_for_host() {
	l_host_spec=$1

	g_zxfer_remote_capability_requested_tools_result=""
	zxfer_append_remote_capability_requested_tool zfs

	if [ -n "${g_option_O_origin_host:-}" ] &&
		{ [ -z "$l_host_spec" ] || [ "$l_host_spec" = "$g_option_O_origin_host" ]; }; then
		if zxfer_remote_capability_origin_should_preload_parallel; then
			zxfer_append_remote_capability_requested_tool parallel
		fi
		if [ "${g_option_e_restore_property_mode:-0}" -eq 1 ]; then
			zxfer_append_remote_capability_requested_tool cat
		fi
		if [ "${g_option_z_compress:-0}" -eq 1 ]; then
			if l_compress_head=$(zxfer_extract_remote_cli_command_head "${g_cmd_compress:-}" "compression command"); then
				zxfer_append_remote_capability_requested_tool "$l_compress_head"
			fi
		fi
	fi

	if [ -n "${g_option_T_target_host:-}" ] &&
		{ [ -z "$l_host_spec" ] || [ "$l_host_spec" = "$g_option_T_target_host" ]; }; then
		if [ "${g_option_k_backup_property_mode:-0}" -eq 1 ]; then
			zxfer_append_remote_capability_requested_tool cat
		fi
		if [ "${g_option_z_compress:-0}" -eq 1 ]; then
			if l_decompress_head=$(zxfer_extract_remote_cli_command_head "${g_cmd_decompress:-}" "decompression command"); then
				zxfer_append_remote_capability_requested_tool "$l_decompress_head"
			fi
		fi
	fi

	printf '%s\n' "$g_zxfer_remote_capability_requested_tools_result"
}

# Purpose: Return the remote capability requested tools for resolving one tool
# in the form expected by later helpers.
# Usage: Called during capability negotiation and remote tool-
# resolution when sibling helpers need the prewarmed host scope when it
# already includes the requested helper.
zxfer_get_remote_capability_requested_tools_for_resolved_tool() {
	l_host_spec=$1
	l_tool=$2

	[ -n "$l_tool" ] || return 1
	if l_host_requested_tools=$(zxfer_get_remote_capability_requested_tools_for_host \
		"$l_host_spec"); then
		case "
$l_host_requested_tools
" in
		*"
$l_tool
"*)
			printf '%s\n' "$l_host_requested_tools"
			return 0
			;;
		esac
	fi

	zxfer_get_remote_capability_requested_tools_for_tool "$l_tool"
}

################################################################################
# PER-RUN REMOTE CAPABILITY STATE / HANDSHAKE PARSING
################################################################################
# Purpose: Render the remote capability cache identity for host as a stable
# shell-safe or operator-facing string.
# Usage: Called during capability negotiation and remote tool-
# resolution when zxfer needs to display or transport the value without
# reparsing it.
zxfer_render_remote_capability_cache_identity_for_host() {
	l_host_spec=$1
	l_requested_tools=${2:-}
	l_cache_role_input=${3:-}
	if ! l_cache_role=$(zxfer_normalize_remote_capability_role "$l_cache_role_input"); then
		return 1
	fi
	l_dependency_path=$(zxfer_get_effective_dependency_path)
	if ! l_transport_policy_identity=$(zxfer_render_ssh_transport_policy_identity); then
		[ "$l_transport_policy_identity" = "" ] || printf '%s\n' "$l_transport_policy_identity"
		return 1
	fi
	if ! zxfer_resolve_remote_capability_requested_tools_for_host \
		"$l_host_spec" "$l_requested_tools" >/dev/null; then
		return 1
	fi

	printf '%s\n%s\n' "$l_dependency_path" "$l_transport_policy_identity"
	[ -z "$l_cache_role" ] || printf 'role=%s\n' "$l_cache_role"
	printf '%s\n' "${g_zxfer_remote_capability_requested_tools_result:-zfs}"
}

# Purpose: Normalize the composition layer's source/destination labels to the
# explicit origin/target identities used by the role-owned capability caches.
# Usage: Optional empty input preserves legacy unassigned-cache helpers; every
# live session lookup supplies a validated role.
zxfer_normalize_remote_capability_role() {
	l_role_input=${1:-}

	case "$l_role_input" in
	'')
		printf '\n'
		;;
	origin | source)
		printf 'origin\n'
		;;
	target | destination)
		printf 'target\n'
		;;
	*)
		return 1
		;;
	esac
}

# Purpose: Parse one fixed capability header line into the response state.
# Usage: Called for the first two payload lines so framing validation remains
# separate from repeated tool-record validation.
zxfer_parse_remote_capability_header_line() {
	l_header_line_number=$1
	l_header_line=$2

	case "$l_header_line_number" in
	1)
		[ "$l_header_line" = "ZXFER_REMOTE_CAPS_V2" ]
		return
		;;
	2)
		case "$l_header_line" in
		os"$l_capability_parse_tab"*)
			g_zxfer_remote_capability_os=${l_header_line#os"$l_capability_parse_tab"}
			[ -n "$g_zxfer_remote_capability_os" ]
			return
			;;
		esac
		;;
	esac

	return 1
}

# Purpose: Validate and publish the fields from one capability tool record.
# Usage: Called for each payload line after the fixed header; result globals
# let the caller append the record without reparsing its tab-delimited fields.
# Side effects: Publishes the validated tool, status, and optional path fields.
zxfer_parse_remote_capability_tool_line() {
	l_capability_record_line=$1
	g_zxfer_remote_capability_record_tool_result=""
	g_zxfer_remote_capability_record_status_result=""
	g_zxfer_remote_capability_record_path_result=""

	if [ "${IFS+set}" = "set" ]; then
		l_capability_record_saved_ifs_set=1
		l_capability_record_saved_ifs=$IFS
	else
		l_capability_record_saved_ifs_set=0
		l_capability_record_saved_ifs=""
	fi
	IFS='	'
	read -r l_capability_record_kind l_capability_record_tool \
		l_capability_record_status l_capability_record_path \
		l_capability_record_extra <<-EOF
			$l_capability_record_line
		EOF
	if [ "$l_capability_record_saved_ifs_set" -eq 1 ]; then
		IFS=$l_capability_record_saved_ifs
	else
		unset IFS
	fi

	[ "$l_capability_record_kind" = "tool" ] || return 1
	[ -z "$l_capability_record_extra" ] || return 1
	case "$l_capability_record_tool" in
	'' | *"$l_capability_parse_tab"* | *"$l_capability_parse_cr"* | *"$l_capability_parse_lf"*)
		return 1
		;;
	esac
	case "$l_capability_record_status" in
	'' | *[!0-9]*)
		return 1
		;;
	esac
	if [ "$l_capability_record_status" -eq 0 ]; then
		[ -n "$l_capability_record_path" ] || return 1
		[ "$l_capability_record_path" != "-" ] || return 1
		(zxfer_validate_resolved_tool_path \
			"$l_capability_record_path" "$l_capability_record_tool" \
			>/dev/null 2>&1) || return 1
	else
		[ "$l_capability_record_path" = "-" ] || return 1
		l_capability_record_path=""
	fi

	g_zxfer_remote_capability_record_tool_result=$l_capability_record_tool
	g_zxfer_remote_capability_record_status_result=$l_capability_record_status
	g_zxfer_remote_capability_record_path_result=$l_capability_record_path
}

# Purpose: Store one validated capability tool record in parsed response state.
# Usage: Called after zxfer_parse_remote_capability_tool_line succeeds so
# duplicate detection and the required zfs status stay centralized.
zxfer_store_parsed_remote_capability_tool_record() {
	l_record_tool=$g_zxfer_remote_capability_record_tool_result
	l_record_status=$g_zxfer_remote_capability_record_status_result
	l_record_path=$g_zxfer_remote_capability_record_path_result

	if zxfer_get_parsed_remote_capability_tool_record "$l_record_tool"; then
		return 1
	fi
	zxfer_append_remote_capability_tool_record \
		"$l_record_tool" "$l_record_status" "$l_record_path" || return 1

	if [ "$l_record_tool" = "zfs" ]; then
		g_zxfer_remote_capability_zfs_status=$l_record_status
	fi
}

# Purpose: Parse one remote capability payload into the structured globals that
# later remote-helper logic consumes.
# Usage: Called after a live or cached capability payload is loaded into the
# current shell.
zxfer_parse_remote_capability_response() {
	l_response=$1
	l_capability_parse_tab='	'
	l_capability_parse_cr=$(printf '\r')
	l_capability_parse_lf=$(printf '\n_')
	l_capability_parse_lf=${l_capability_parse_lf%_}

	zxfer_reset_remote_capability_parse_state
	case "$l_response" in
	*'
')
		l_response=${l_response%?}
		;;
	esac

	l_line_number=0
	l_tool_count=0
	l_capability_end_seen=0
	while IFS= read -r l_line || [ -n "$l_line" ]; do
		l_line_number=$((l_line_number + 1))
		if [ "$l_line_number" -le 2 ]; then
			zxfer_parse_remote_capability_header_line \
				"$l_line_number" "$l_line" || return 1
			continue
		fi
		[ "$l_capability_end_seen" -eq 0 ] || return 1
		if [ "$l_line" = "end" ]; then
			l_capability_end_seen=1
			continue
		fi

		zxfer_parse_remote_capability_tool_line "$l_line" || return 1
		zxfer_store_parsed_remote_capability_tool_record || return 1
		l_tool_count=$((l_tool_count + 1))
	done <<-EOF
		$l_response
	EOF

	[ "$l_line_number" -ge 3 ] || return 1
	[ -n "$g_zxfer_remote_capability_os" ] || return 1
	[ "${l_tool_count:-0}" -gt 0 ] || return 1
	[ -n "$g_zxfer_remote_capability_zfs_status" ] || return 1
	[ "$l_capability_end_seen" -eq 1 ] || return 1
	return 0
}

# Purpose: Require a parsed capability payload to contain every tool in the
# negotiated request scope. The parser already rejects duplicate records, so
# this proves each requested tool appears exactly once and detects truncation.
# Usage: Called before accepting either a live or in-memory response.
zxfer_parsed_remote_capabilities_cover_requested_tools() {
	l_coverage_host_spec=$1
	l_coverage_requested_tools=${2:-}

	if ! zxfer_resolve_remote_capability_requested_tools_for_host \
		"$l_coverage_host_spec" "$l_coverage_requested_tools" >/dev/null; then
		return 1
	fi
	l_coverage_tools=$g_zxfer_remote_capability_requested_tools_result
	while IFS= read -r l_coverage_tool || [ -n "$l_coverage_tool" ]; do
		[ -n "$l_coverage_tool" ] || continue
		zxfer_get_parsed_remote_capability_tool_record \
			"$l_coverage_tool" || return 1
	done <<EOF
$l_coverage_tools
EOF

	return 0
}

# Purpose: Reset the remote probe capture state so the next remote-host pass
# starts from a clean state.
# Usage: Called during capability negotiation and remote tool-
# resolution before this module reuses mutable scratch globals or cached
# decisions.
#
# Run a remote shell probe while preserving stdout and stderr separately in the
# current shell. The probe helpers use the captured stderr to surface ssh,
# bootstrap, or host-authentication failures instead of collapsing them into a
# generic dependency lookup error.
zxfer_reset_remote_probe_capture_state() {
	g_zxfer_remote_probe_stdout=""
	g_zxfer_remote_probe_stderr=""
	g_zxfer_remote_probe_capture_read_result=""
	g_zxfer_remote_probe_capture_failed=0
}

# Purpose: Read the remote probe capture file from staged state into the
# current shell.
# Usage: Called during capability negotiation and remote tool-
# resolution when later helpers need a checked reload instead of ad hoc
# file reads.
zxfer_read_remote_probe_capture_file() {
	l_capture_path=$1

	g_zxfer_remote_probe_capture_read_result=""
	if zxfer_read_runtime_artifact_file "$l_capture_path" >/dev/null; then
		g_zxfer_remote_probe_capture_read_result=$g_zxfer_runtime_artifact_read_result
	else
		l_read_status=$?
		return "$l_read_status"
	fi

	printf '%s\n' "$g_zxfer_remote_probe_capture_read_result"
}

# Purpose: Load the remote probe capture files from the module-owned cache or
# staged source.
# Usage: Called during capability negotiation and remote tool-
# resolution when later helpers need a checked in-memory copy of staged
# data.
zxfer_load_remote_probe_capture_files() {
	l_capture_label=$1
	l_stdout_path=$2
	l_stderr_path=$3

	g_zxfer_remote_probe_stdout=""
	g_zxfer_remote_probe_stderr=""
	g_zxfer_remote_probe_capture_failed=0

	zxfer_read_remote_probe_capture_file "$l_stdout_path" >/dev/null
	l_stdout_read_status=$?
	l_stdout_contents=$g_zxfer_remote_probe_capture_read_result

	zxfer_read_remote_probe_capture_file "$l_stderr_path" >/dev/null
	l_stderr_read_status=$?
	l_stderr_contents=$g_zxfer_remote_probe_capture_read_result

	if [ "$l_stdout_read_status" -eq 0 ] && [ "$l_stderr_read_status" -eq 0 ]; then
		g_zxfer_remote_probe_stdout=$l_stdout_contents
		g_zxfer_remote_probe_stderr=$l_stderr_contents
		return 0
	fi

	g_zxfer_remote_probe_capture_failed=1
	case "${l_stdout_read_status}:${l_stderr_read_status}" in
	0:*)
		g_zxfer_remote_probe_stderr="Failed to read $l_capture_label stderr capture from local staging."
		return "$l_stderr_read_status"
		;;
	*:0)
		g_zxfer_remote_probe_stderr="Failed to read $l_capture_label stdout capture from local staging."
		return "$l_stdout_read_status"
		;;
	*)
		g_zxfer_remote_probe_stderr="Failed to read $l_capture_label stdout and stderr capture from local staging."
		return "$l_stdout_read_status"
		;;
	esac
}

# Purpose: Capture the remote probe output into staged state or module globals
# for later use.
# Usage: Called during capability negotiation and remote tool-
# resolution when later helpers need a checked snapshot of command
# output or computed state.
zxfer_capture_remote_probe_output() {
	l_host_spec=$1
	l_remote_probe_cmd=$2
	l_profile_side=${3:-}

	zxfer_reset_remote_probe_capture_state

	if l_transport_tokens=$(zxfer_get_ssh_transport_tokens_for_host "$l_host_spec"); then
		:
	else
		zxfer_profile_record_ssh_invocation "$l_host_spec" "$l_profile_side"
		l_transport_status=$?
		zxfer_throw_error "$l_transport_tokens" "$l_transport_status"
	fi

	l_temp_prefix="${g_zxfer_temp_prefix:-zxfer.$$.${g_option_Y_yield_iterations:-1}.$(date +%s)}.remote-probe"
	zxfer_create_private_temp_dir "$l_temp_prefix" >/dev/null
	l_capture_status=$?
	if [ "$l_capture_status" -ne 0 ]; then
		zxfer_throw_error "Error creating temporary file."
	fi
	l_capture_dir=$g_zxfer_runtime_artifact_path_result
	l_stdout_path="$l_capture_dir/stdout"
	l_stderr_path="$l_capture_dir/stderr"
	if [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
		zxfer_echoV "Running remote probe [$(zxfer_get_remote_command_context_label "$l_host_spec" "$l_profile_side")]: $l_remote_probe_cmd"
	fi

	if zxfer_invoke_ssh_shell_command_for_host \
		"$l_host_spec" "$l_remote_probe_cmd" "$l_profile_side" >"$l_stdout_path" 2>"$l_stderr_path"; then
		l_remote_status=0
	else
		l_remote_status=$?
	fi

	zxfer_load_remote_probe_capture_files "remote probe" "$l_stdout_path" "$l_stderr_path"
	l_capture_status=$?
	zxfer_cleanup_runtime_artifact_path "$l_capture_dir"
	if [ "$l_capture_status" -ne 0 ]; then
		return "$l_capture_status"
	fi
	return "$l_remote_status"
}

# Purpose: Emit the remote probe failure message in the operator-facing format
# owned by this module.
# Usage: Called during capability negotiation and remote tool-
# resolution when zxfer needs to surface status, warning, or diagnostic
# text.
zxfer_emit_remote_probe_failure_message() {
	l_default_message=${1:-}

	if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
		printf '%s\n' "$g_zxfer_remote_probe_stderr"
		return 0
	fi
	[ -z "$l_default_message" ] || printf '%s\n' "$l_default_message"
}

# Purpose: Return the cached remote capability response for host in the form
# expected by later helpers.
# Usage: Called during capability negotiation and remote tool-
# resolution when sibling helpers need the same lookup without
# duplicating module logic.
zxfer_get_cached_remote_capability_response_for_host() {
	l_cache_get_host_spec=$1
	l_cache_get_requested_tools=${2:-}
	l_cache_get_role_input=${3:-}
	if ! l_cache_get_role=$(zxfer_normalize_remote_capability_role \
		"$l_cache_get_role_input"); then
		return 1
	fi
	if ! l_cache_get_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"$l_cache_get_host_spec" "$l_cache_get_requested_tools" \
		"$l_cache_get_role"); then
		return 1
	fi

	if [ "$l_cache_get_role" != target ] &&
		[ "$l_cache_get_host_spec" = "${g_origin_remote_capabilities_host:-}" ] &&
		[ "$l_cache_get_identity" = "${g_origin_remote_capabilities_cache_identity:-}" ] &&
		[ -n "${g_origin_remote_capabilities_response:-}" ]; then
		printf '%s\n' "$g_origin_remote_capabilities_response"
		return 0
	fi

	if [ "$l_cache_get_role" != origin ] &&
		[ "$l_cache_get_host_spec" = "${g_target_remote_capabilities_host:-}" ] &&
		[ "$l_cache_get_identity" = "${g_target_remote_capabilities_cache_identity:-}" ] &&
		[ -n "${g_target_remote_capabilities_response:-}" ]; then
		printf '%s\n' "$g_target_remote_capabilities_response"
		return 0
	fi

	return 1
}

# Purpose: Store the cached remote capability response for host in the cache or
# staging location owned by this module.
# Usage: Called during capability negotiation and remote tool-
# resolution after zxfer has a validated value that later helpers may
# reuse.
zxfer_store_cached_remote_capability_response_for_host() {
	l_cache_store_host_spec=$1
	l_cache_store_response=$2
	l_cache_store_requested_tools=${3:-}
	l_cache_store_role_input=${4:-}
	if ! l_cache_store_role=$(zxfer_normalize_remote_capability_role \
		"$l_cache_store_role_input"); then
		return 1
	fi
	l_cache_store_stored=0
	if ! l_cache_store_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"$l_cache_store_host_spec" "$l_cache_store_requested_tools" \
		"$l_cache_store_role"); then
		l_cache_store_identity=""
	fi

	if [ "$l_cache_store_role" = origin ] ||
		{ [ -z "$l_cache_store_role" ] &&
			{ [ "$l_cache_store_host_spec" = "${g_option_O_origin_host:-}" ] ||
				[ "$l_cache_store_host_spec" = "${g_origin_remote_capabilities_host:-}" ]; }; }; then
		if [ "${g_origin_remote_capabilities_cache_identity:-}" != "$l_cache_store_identity" ] ||
			[ "${g_origin_remote_capabilities_host:-}" != "$l_cache_store_host_spec" ]; then
			g_origin_remote_capabilities_bootstrap_source=""
		fi
		g_origin_remote_capabilities_host=$l_cache_store_host_spec
		g_origin_remote_capabilities_cache_identity=$l_cache_store_identity
		g_origin_remote_capabilities_response=$l_cache_store_response
		l_cache_store_stored=1
	fi

	if [ "$l_cache_store_role" = target ] ||
		{ [ -z "$l_cache_store_role" ] &&
			{ [ "$l_cache_store_host_spec" = "${g_option_T_target_host:-}" ] ||
				[ "$l_cache_store_host_spec" = "${g_target_remote_capabilities_host:-}" ]; }; }; then
		if [ "${g_target_remote_capabilities_cache_identity:-}" != "$l_cache_store_identity" ] ||
			[ "${g_target_remote_capabilities_host:-}" != "$l_cache_store_host_spec" ]; then
			g_target_remote_capabilities_bootstrap_source=""
		fi
		g_target_remote_capabilities_host=$l_cache_store_host_spec
		g_target_remote_capabilities_cache_identity=$l_cache_store_identity
		g_target_remote_capabilities_response=$l_cache_store_response
		l_cache_store_stored=1
	fi

	if [ "$l_cache_store_stored" -eq 0 ] &&
		[ "${g_origin_remote_capabilities_host:-}" = "" ]; then
		g_origin_remote_capabilities_host=$l_cache_store_host_spec
		g_origin_remote_capabilities_cache_identity=$l_cache_store_identity
		g_origin_remote_capabilities_response=$l_cache_store_response
		g_origin_remote_capabilities_bootstrap_source=""
		return
	fi

	if [ "$l_cache_store_stored" -eq 0 ]; then
		g_target_remote_capabilities_host=$l_cache_store_host_spec
		g_target_remote_capabilities_cache_identity=$l_cache_store_identity
		g_target_remote_capabilities_response=$l_cache_store_response
		g_target_remote_capabilities_bootstrap_source=""
	fi
}

# Purpose: Record the remote capability bootstrap source for host for later
# diagnostics or control decisions.
# Usage: Called during capability negotiation and remote tool-
# resolution when zxfer needs the state preserved for follow-on helpers
# or reporting.
zxfer_note_remote_capability_bootstrap_source_for_host() {
	l_cache_note_host_spec=$1
	l_cache_note_source=$2
	l_cache_note_requested_tools=${3:-}
	l_cache_note_role_input=${4:-}
	if ! l_cache_note_role=$(zxfer_normalize_remote_capability_role \
		"$l_cache_note_role_input"); then
		return 0
	fi
	if ! l_cache_note_identity=$(zxfer_render_remote_capability_cache_identity_for_host \
		"$l_cache_note_host_spec" "$l_cache_note_requested_tools" \
		"$l_cache_note_role"); then
		return 0
	fi

	[ -n "$l_cache_note_host_spec" ] || return 0
	[ -n "$l_cache_note_source" ] || return 0

	if [ "$l_cache_note_role" != target ] &&
		{ { [ "$l_cache_note_host_spec" = "${g_option_O_origin_host:-}" ] &&
			{ [ "${g_origin_remote_capabilities_cache_identity:-}" = "" ] ||
				[ "$l_cache_note_identity" = "${g_origin_remote_capabilities_cache_identity:-}" ]; }; } ||
			{ [ "$l_cache_note_host_spec" = "${g_origin_remote_capabilities_host:-}" ] &&
				[ "$l_cache_note_identity" = "${g_origin_remote_capabilities_cache_identity:-}" ]; }; }; then
		if [ "${g_origin_remote_capabilities_bootstrap_source:-}" = "" ]; then
			g_origin_remote_capabilities_bootstrap_source=$l_cache_note_source
		fi
	fi

	if [ "$l_cache_note_role" != origin ] &&
		{ { [ "$l_cache_note_host_spec" = "${g_option_T_target_host:-}" ] &&
			{ [ "${g_target_remote_capabilities_cache_identity:-}" = "" ] ||
				[ "$l_cache_note_identity" = "${g_target_remote_capabilities_cache_identity:-}" ]; }; } ||
			{ [ "$l_cache_note_host_spec" = "${g_target_remote_capabilities_host:-}" ] &&
				[ "$l_cache_note_identity" = "${g_target_remote_capabilities_cache_identity:-}" ]; }; }; then
		if [ "${g_target_remote_capabilities_bootstrap_source:-}" = "" ]; then
			g_target_remote_capabilities_bootstrap_source=$l_cache_note_source
		fi
	fi
}

# Purpose: Build the remote capability probe script for the next execution or
# comparison step.
# Usage: Called during capability negotiation and remote tool-
# resolution before other helpers consume the assembled value.
zxfer_build_remote_capability_probe_script() {
	l_host_spec=$1
	l_requested_tools=${2:-}

	l_dependency_path=$(zxfer_get_effective_dependency_path)
	l_dependency_path_single=$(zxfer_escape_for_single_quotes "$l_dependency_path")
	if ! zxfer_resolve_remote_capability_requested_tools_for_host \
		"$l_host_spec" "$l_requested_tools" >/dev/null; then
		return 1
	fi
	l_requested_tool_tokens=$(zxfer_quote_token_stream \
		"${g_zxfer_remote_capability_requested_tools_result:-zfs}")
	[ "$l_requested_tool_tokens" != "" ] || l_requested_tool_tokens="'zfs'"

	printf "%s\n" "PATH='$l_dependency_path_single'; export PATH; l_os=\$(uname 2>/dev/null) || exit \$?; printf '%s\n' 'ZXFER_REMOTE_CAPS_V2'; printf '%s\t%s\n' 'os' \"\$l_os\"; for l_tool in $l_requested_tool_tokens; do [ -n \"\$l_tool\" ] || continue; l_path=\$(command -v \"\$l_tool\" 2>/dev/null); l_status=\$?; if [ \"\$l_status\" -eq 0 ]; then printf '%s\t%s\t0\t%s\n' 'tool' \"\$l_tool\" \"\$l_path\"; elif [ \"\$l_status\" -eq 1 ]; then printf '%s\t%s\t1\t-\n' 'tool' \"\$l_tool\"; else printf '%s\t%s\t%s\t-\n' 'tool' \"\$l_tool\" \"\$l_status\"; fi; done; printf '%s\n' 'end'"
}

# Purpose: Probe a remote host live for the capability payload that describes
# its helper and platform state.
# Usage: Called during capability negotiation and remote tool-
# resolution when cached capability data is missing or invalid.
zxfer_fetch_remote_host_capabilities_live() {
	l_host_spec=$1
	l_profile_side=${2:-}
	l_requested_tools=${3:-}

	g_zxfer_remote_capability_response_result=""
	[ -n "$l_host_spec" ] || return 1

	if ! l_remote_probe=$(zxfer_build_remote_capability_probe_script \
		"$l_host_spec" "$l_requested_tools"); then
		return 1
	fi
	l_remote_probe_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_probe")
	if ! zxfer_capture_remote_probe_output "$l_host_spec" "$l_remote_probe_cmd" "$l_profile_side"; then
		zxfer_emit_remote_probe_failure_message >&2
		return 1
	fi
	l_remote_output=$g_zxfer_remote_probe_stdout

	zxfer_parse_remote_capability_response "$l_remote_output" || return 1
	zxfer_parsed_remote_capabilities_cover_requested_tools \
		"$l_host_spec" "$l_requested_tools" || return 1

	g_zxfer_remote_capability_response_result=$l_remote_output
	printf '%s\n' "$l_remote_output"
}

# Purpose: Ensure the remote host capabilities exist and are ready before the
# flow continues.
# Usage: Called during remote bootstrap, capability probing, and ssh control-
# socket management before later helpers assume the capability payload is
# available. Capability state is per-run only: one live probe per host fills
# the in-memory tier and every later lookup in this run is answered from
# memory.
zxfer_ensure_remote_host_capabilities() {
	l_host_spec=$1
	l_profile_side=${2:-}
	l_requested_tools=${3:-}

	g_zxfer_remote_capability_response_result=""
	[ -n "$l_host_spec" ] || return 1

	if l_cached_response=$(zxfer_get_cached_remote_capability_response_for_host \
		"$l_host_spec" "$l_requested_tools" "$l_profile_side"); then
		if zxfer_parse_remote_capability_response "$l_cached_response" &&
			zxfer_parsed_remote_capabilities_cover_requested_tools \
				"$l_host_spec" "$l_requested_tools"; then
			zxfer_note_remote_capability_bootstrap_source_for_host \
				"$l_host_spec" memory "$l_requested_tools" "$l_profile_side"
			zxfer_profile_record_remote_capability_bootstrap_source memory
			g_zxfer_remote_capability_response_result=$l_cached_response
			printf '%s\n' "$l_cached_response"
			return 0
		fi
	fi

	if zxfer_fetch_remote_host_capabilities_live \
		"$l_host_spec" "$l_profile_side" "$l_requested_tools" >/dev/null; then
		:
	else
		l_live_status=$?
		return "$l_live_status"
	fi
	l_live_response=$g_zxfer_remote_capability_response_result

	zxfer_store_cached_remote_capability_response_for_host \
		"$l_host_spec" "$l_live_response" "$l_requested_tools" "$l_profile_side"
	zxfer_note_remote_capability_bootstrap_source_for_host \
		"$l_host_spec" live "$l_requested_tools" "$l_profile_side"
	zxfer_profile_record_remote_capability_bootstrap_source live
	g_zxfer_remote_capability_response_result=$l_live_response
	printf '%s\n' "$l_live_response"
}

# Purpose: Preload the remote host capabilities before later helpers need them.
# Usage: Called during capability negotiation and remote tool-
# resolution when zxfer wants startup or iteration work to resolve
# expensive state ahead of time.
zxfer_preload_remote_host_capabilities() {
	l_host_spec=$1
	l_profile_side=${2:-}
	if ! l_requested_tools=$(zxfer_get_remote_capability_requested_tools_for_host \
		"$l_host_spec"); then
		l_requested_tools=$(zxfer_get_remote_capability_requested_tools_for_tool zfs)
	fi

	if [ "${g_option_v_verbose:-0}" -eq 1 ] || [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
		zxfer_ensure_remote_host_capabilities \
			"$l_host_spec" "$l_profile_side" "$l_requested_tools" >/dev/null
		return "$?"
	fi

	zxfer_ensure_remote_host_capabilities \
		"$l_host_spec" "$l_profile_side" "$l_requested_tools" >/dev/null 2>&1
}

# Purpose: Return the remote host operating system direct in the form expected
# by later helpers.
# Usage: Called during capability negotiation and remote tool-
# resolution when sibling helpers need the same lookup without
# duplicating module logic.
zxfer_get_remote_host_operating_system_direct() {
	l_host_spec=$1
	l_profile_side=${2:-}

	l_dependency_path=$(zxfer_get_effective_dependency_path)
	l_dependency_path_single=$(zxfer_escape_for_single_quotes "$l_dependency_path")
	l_remote_probe="PATH='$l_dependency_path_single'; export PATH; uname 2>/dev/null"
	l_remote_probe_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_probe")
	if ! zxfer_capture_remote_probe_output "$l_host_spec" "$l_remote_probe_cmd" "$l_profile_side"; then
		zxfer_emit_remote_probe_failure_message
		return 1
	fi
	l_remote_output=$g_zxfer_remote_probe_stdout

	l_remote_os=$(printf '%s\n' "$l_remote_output" | sed -n '1p')
	[ -n "$l_remote_os" ] || return 1
	printf '%s\n' "$l_remote_os"
}

# Purpose: Return the remote host operating system in the form expected by
# later helpers.
# Usage: Called during capability negotiation and remote tool-
# resolution when sibling helpers need the same lookup without
# duplicating module logic.
zxfer_get_remote_host_operating_system() {
	l_host_spec=$1
	l_profile_side=${2:-}
	if ! l_requested_tools=$(zxfer_get_remote_capability_requested_tools_for_host "$l_host_spec"); then
		l_requested_tools=$(zxfer_get_remote_capability_requested_tools_for_tool zfs)
	fi

	if ! l_response=$(zxfer_ensure_remote_host_capabilities \
		"$l_host_spec" "$l_profile_side" "$l_requested_tools"); then
		if ! l_fallback_os=$(zxfer_get_remote_host_operating_system_direct "$l_host_spec" "$l_profile_side"); then
			[ "$l_fallback_os" = "" ] || printf '%s\n' "$l_fallback_os"
			return 1
		fi
		printf '%s\n' "$l_fallback_os"
		return 0
	fi
	if ! zxfer_parse_remote_capability_response "$l_response"; then
		if ! l_fallback_os=$(zxfer_get_remote_host_operating_system_direct "$l_host_spec" "$l_profile_side"); then
			[ "$l_fallback_os" = "" ] || printf '%s\n' "$l_fallback_os"
			return 1
		fi
		printf '%s\n' "$l_fallback_os"
		return 0
	fi
	printf '%s\n' "$g_zxfer_remote_capability_os"
}

# Purpose: Return the operating system for a local or remote execution role.
# Usage: Called during session execution-context initialization so the local
# uname path and capability-backed remote path share one status contract.
# Returns: The operating-system name on stdout, preserving remote failures.
zxfer_get_os() {
	l_host_spec=$1
	l_profile_side=${2:-}
	l_output_os=""

	if [ "$l_host_spec" = "" ]; then
		l_output_os=$(uname)
	else
		l_output_os=$(zxfer_get_remote_host_operating_system \
			"$l_host_spec" "$l_profile_side") || return "$?"
	fi

	printf '%s\n' "$l_output_os"
}

################################################################################
# REMOTE TOOL / COMMAND RESOLUTION
################################################################################

# Purpose: Emit the missing remote dependency message in the operator-facing
# format owned by this module.
# Usage: Called during capability negotiation and remote tool-
# resolution when zxfer needs to surface status, warning, or diagnostic
# text.
zxfer_print_missing_remote_dependency_message() {
	l_host=$1
	l_label=$2
	l_dependency_path=$(zxfer_get_effective_dependency_path)

	printf '%s\n' "Required dependency \"$l_label\" not found on host $l_host in secure PATH ($l_dependency_path). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
}

# Purpose: Resolve the effective remote tool from parsed capabilities that
# zxfer should use.
# Usage: Called during capability negotiation and remote tool-
# resolution after configuration, cache state, or remote state can
# change the final choice.
#
# Resolve a tool from the parsed remote-capability payload already loaded into
# the current shell. Return status 2 when the tool is absent from the payload
# so callers can fall back to a direct secure probe.
zxfer_resolve_remote_tool_from_parsed_capabilities() {
	l_host=$1
	l_tool=$2
	l_label=${3:-$l_tool}

	[ -n "$l_host" ] || return 1
	[ -n "$l_tool" ] || return 1

	zxfer_get_parsed_remote_capability_tool_record "$l_tool" || return 2

	case "$g_zxfer_remote_capability_tool_status_result" in
	0)
		zxfer_validate_resolved_tool_path \
			"$g_zxfer_remote_capability_tool_path_result" \
			"$l_label" \
			"host $l_host"
		;;
	1)
		zxfer_print_missing_remote_dependency_message "$l_host" "$l_label"
		return 1
		;;
	*)
		printf '%s\n' "Failed to query dependency \"$l_label\" on host $l_host."
		return 1
		;;
	esac
}

# Purpose: Resolve the effective remote required tool that zxfer should use.
# Usage: Called during capability negotiation and remote tool-
# resolution after configuration, cache state, or remote state can
# change the final choice.
zxfer_resolve_remote_required_tool() {
	l_host=$1
	l_tool=$2
	l_label=${3:-$l_tool}
	l_profile_side=${4:-}

	[ -n "$l_host" ] || return 1

	# The combined capability protocol only has records for zxfer's fixed set
	# of remote dependencies. Reject any other name before opening an SSH
	# connection or attempting the direct-probe fallback.
	case "$l_tool" in
	zfs | parallel | cat) ;;
	*)
		printf '%s\n' "Failed to query dependency \"$l_label\" on host $l_host."
		return 1
		;;
	esac

	l_requested_tools=$(zxfer_get_remote_capability_requested_tools_for_resolved_tool \
		"$l_host" "$l_tool")

	if ! l_remote_caps=$(zxfer_ensure_remote_host_capabilities \
		"$l_host" "$l_profile_side" "$l_requested_tools"); then
		if l_fallback_path=$(zxfer_resolve_remote_cli_tool_direct "$l_host" "$l_tool" "$l_label" "$l_profile_side"); then
			printf '%s\n' "$l_fallback_path"
			return 0
		fi
		printf '%s\n' "$l_fallback_path"
		return 1
	fi
	if ! zxfer_parse_remote_capability_response "$l_remote_caps"; then
		if l_fallback_path=$(zxfer_resolve_remote_cli_tool_direct "$l_host" "$l_tool" "$l_label" "$l_profile_side"); then
			printf '%s\n' "$l_fallback_path"
			return 0
		fi
		printf '%s\n' "$l_fallback_path"
		return 1
	fi

	l_resolved_path=$(zxfer_resolve_remote_tool_from_parsed_capabilities \
		"$l_host" "$l_tool" "$l_label")
	l_resolve_status=$?
	if [ "$l_resolve_status" -eq 0 ]; then
		printf '%s\n' "$l_resolved_path"
		return 0
	fi
	case "$l_resolve_status" in
	2)
		if l_fallback_path=$(zxfer_resolve_remote_cli_tool_direct \
			"$l_host" "$l_tool" "$l_label" "$l_profile_side"); then
			printf '%s\n' "$l_fallback_path"
			return 0
		fi
		printf '%s\n' "$l_fallback_path"
		return 1
		;;
	*)
		printf '%s\n' "$l_resolved_path"
		return 1
		;;
	esac
}

# Purpose: Resolve the effective remote CLI tool direct that zxfer should use.
# Usage: Called during capability negotiation and remote tool-
# resolution after configuration, cache state, or remote state can
# change the final choice.
zxfer_resolve_remote_cli_tool_direct() {
	l_host=$1
	l_tool=$2
	l_label=${3:-$l_tool}
	l_profile_side=${4:-}

	zxfer_profile_increment_counter g_zxfer_profile_remote_cli_tool_direct_probes
	l_dependency_path=$(zxfer_get_effective_dependency_path)
	l_dependency_path_single=$(zxfer_escape_for_single_quotes "$l_dependency_path")
	l_tool_single=$(zxfer_escape_for_single_quotes "$l_tool")
	l_remote_probe="PATH='$l_dependency_path_single'; export PATH; l_path=\$(command -v '$l_tool_single' 2>/dev/null); l_status=\$?; if [ \"\$l_status\" -eq 0 ]; then printf '%s\n' \"\$l_path\"; elif [ \"\$l_status\" -eq 1 ]; then exit 10; else exit \"\$l_status\"; fi"
	l_remote_probe_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_probe")
	if zxfer_capture_remote_probe_output "$l_host" "$l_remote_probe_cmd" "$l_profile_side"; then
		l_remote_status=0
	else
		l_remote_status=$?
	fi
	l_remote_output=$g_zxfer_remote_probe_stdout

	case "$l_remote_status" in
	0)
		zxfer_validate_resolved_tool_path "$l_remote_output" "$l_label" "host $l_host"
		;;
	10)
		zxfer_print_missing_remote_dependency_message "$l_host" "$l_label"
		return 1
		;;
	*)
		zxfer_emit_remote_probe_failure_message \
			"Failed to query dependency \"$l_label\" on host $l_host."
		return 1
		;;
	esac
}

# Purpose: Resolve the effective remote CLI tool that zxfer should use.
# Usage: Called during capability negotiation and remote tool-
# resolution after configuration, cache state, or remote state can
# change the final choice.
zxfer_resolve_remote_cli_tool() {
	l_host=$1
	l_tool=$2
	l_label=${3:-$l_tool}
	l_profile_side=${4:-}
	l_requested_tools=$(zxfer_get_remote_capability_requested_tools_for_resolved_tool \
		"$l_host" "$l_tool")

	case "$l_tool" in
	zfs | parallel | cat)
		zxfer_resolve_remote_required_tool "$l_host" "$l_tool" "$l_label" "$l_profile_side"
		return
		;;
	esac

	if ! l_remote_caps=$(zxfer_ensure_remote_host_capabilities \
		"$l_host" "$l_profile_side" "$l_requested_tools"); then
		zxfer_resolve_remote_cli_tool_direct "$l_host" "$l_tool" "$l_label" "$l_profile_side"
		return
	fi
	if ! zxfer_parse_remote_capability_response "$l_remote_caps"; then
		zxfer_resolve_remote_cli_tool_direct "$l_host" "$l_tool" "$l_label" "$l_profile_side"
		return
	fi

	l_resolved_path=$(zxfer_resolve_remote_tool_from_parsed_capabilities \
		"$l_host" "$l_tool" "$l_label")
	l_resolve_status=$?
	if [ "$l_resolve_status" -eq 0 ]; then
		printf '%s\n' "$l_resolved_path"
		return 0
	fi
	case "$l_resolve_status" in
	2)
		zxfer_resolve_remote_cli_tool_direct "$l_host" "$l_tool" "$l_label" "$l_profile_side"
		;;
	*)
		printf '%s\n' "$l_resolved_path"
		return 1
		;;
	esac
}

# Purpose: Resolve the effective remote CLI command safe that zxfer should use.
# Usage: Called during capability negotiation and remote tool-
# resolution after configuration, cache state, or remote state can
# change the final choice.
zxfer_resolve_remote_cli_command_safe() {
	l_host=$1
	l_cli_string=$2
	l_label=${3:-command}
	l_profile_side=${4:-}
	if ! l_cli_tokens=$(zxfer_split_cli_tokens "$l_cli_string" "$l_label"); then
		printf '%s\n' "$l_cli_tokens"
		return 1
	fi
	l_cli_head=$(printf '%s\n' "$l_cli_tokens" | sed -n '1p')
	if [ -z "$l_cli_head" ]; then
		printf '%s\n' "Required dependency \"$l_label\" must not be empty or whitespace-only."
		return 1
	fi

	if ! l_resolved_head=$(zxfer_resolve_remote_cli_tool "$l_host" "$l_cli_head" "$l_label" "$l_profile_side"); then
		printf '%s\n' "$l_resolved_head"
		return 1
	fi

	zxfer_requote_cli_command_with_resolved_head "$l_cli_string" "$l_resolved_head" "$l_label"
}
