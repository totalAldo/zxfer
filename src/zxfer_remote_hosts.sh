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
# owns globals: one capability slot per role, g_origin_remote_capabilities_*
#   and g_target_remote_capabilities_* (host, tools, response, os, zfs_status,
#   tool_records); the active parsed channel g_zxfer_remote_capability_*; the
#   probe results g_zxfer_remote_probe_stdout, _stderr and _capture_failed;
#   and each endpoint's g_{source,destination}_operating_system and
#   g_{origin,target}_cmd_zfs.
# reads globals: -O/-T and the options that shape the tool scope (-j, -e, -k,
#   -z with g_cmd_compress/g_cmd_decompress), g_zxfer_secure_path, and the ssh
#   transport, runtime temp-file and profile helpers.
# mutates caches: fills a role's slot once per host and tool scope, only after
#   a validated live probe; either role's slot answers a lookup with the same
#   host and scope. Secure PATH and ssh policy are fixed per process, so they
#   are not part of the key.
# returns via stdout: the capability response
#   (zxfer_ensure_remote_host_capabilities), the OS name (zxfer_get_os) and
#   probe failure messages.

# Purpose: Reset the capability slots, probe results and endpoint contexts.
# Usage: Called by zxfer_reset_session_state. g_origin_cmd_zfs and
# g_target_cmd_zfs copy g_cmd_zfs, which stays empty until
# zxfer_init_session_environment resolves it.
zxfer_reset_remote_host_state() {
	g_origin_remote_capabilities_host=""
	g_origin_remote_capabilities_tools=""
	g_origin_remote_capabilities_response=""
	g_origin_remote_capabilities_os=""
	g_origin_remote_capabilities_zfs_status=""
	g_origin_remote_capabilities_tool_records=""
	g_target_remote_capabilities_host=""
	g_target_remote_capabilities_tools=""
	g_target_remote_capabilities_response=""
	g_target_remote_capabilities_os=""
	g_target_remote_capabilities_zfs_status=""
	g_target_remote_capabilities_tool_records=""
	g_zxfer_remote_capability_response_result=""
	g_zxfer_remote_capability_os=""
	g_zxfer_remote_capability_zfs_status=""
	g_zxfer_remote_capability_tool_records=""
	g_zxfer_remote_capability_tool_status_result=""
	g_zxfer_remote_capability_tool_path_result=""
	g_zxfer_remote_capability_requested_tools_result=""
	g_zxfer_remote_capability_probe_script_result=""
	g_zxfer_remote_probe_stdout=""
	g_zxfer_remote_probe_stderr=""
	g_zxfer_remote_probe_capture_failed=0
	g_source_operating_system=""
	g_destination_operating_system=""
	g_origin_cmd_zfs=$g_cmd_zfs
	g_target_cmd_zfs=$g_cmd_zfs
}

# Purpose: Publish one endpoint's resolved operating system and zfs command.
# Usage: zxfer_publish_endpoint_runtime_context origin|target OS ZFS_COMMAND;
# returns 2 for any other role.
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

################################################################################
# TOOL SCOPE
################################################################################

# Purpose: Decide whether the active options permit the clean recursive no-op
# proof before full source discovery.
# Usage: zxfer_fast_recursive_noop_options_are_eligible; shared by the origin
# tool scope and snapshot discovery so the safety gates live in one place.
zxfer_fast_recursive_noop_options_are_eligible() {
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

# Purpose: Work out which helpers one capability probe asks a host about.
# Usage: zxfer_get_remote_capability_requested_tools_for_host HOST_SPEC [TOOL];
# publishes the space-separated list in
# g_zxfer_remote_capability_requested_tools_result. The list starts with zfs.
# The -O host adds parallel (-j above 1, unless the fast no-op proof runs
# instead), cat (-e) and the compression command (-z); the -T host adds cat
# (-k) and the decompression command (-z); a host that is both gets the union.
# A TOOL outside that scope narrows the list to "zfs TOOL".
zxfer_get_remote_capability_requested_tools_for_host() {
	l_scope_host=$1
	l_scope_tool=${2:-}
	l_scope_candidates=""

	# A compression command's helper is its first token, split as
	# zxfer_resolve_cli_command_safe splits it; a command it would reject adds
	# nothing.
	if [ -n "${g_option_O_origin_host:-}" ] && [ "$l_scope_host" = "$g_option_O_origin_host" ]; then
		if [ "${g_option_j_jobs:-1}" -gt 1 ] &&
			! zxfer_fast_recursive_noop_options_are_eligible; then
			l_scope_candidates="$l_scope_candidates parallel"
		fi
		[ "${g_option_e_restore_property_mode:-0}" -ne 1 ] ||
			l_scope_candidates="$l_scope_candidates cat"
		if [ "${g_option_z_compress:-0}" -eq 1 ] &&
			zxfer_check_literal_token_string "${g_cmd_compress:-}"; then
			zxfer_split_tokens_into_result "${g_cmd_compress:-}"
			l_scope_candidates="$l_scope_candidates ${g_zxfer_split_tokens_result%%"$ZXFER_LF"*}"
		fi
	fi
	if [ -n "${g_option_T_target_host:-}" ] && [ "$l_scope_host" = "$g_option_T_target_host" ]; then
		[ "${g_option_k_backup_property_mode:-0}" -ne 1 ] ||
			l_scope_candidates="$l_scope_candidates cat"
		if [ "${g_option_z_compress:-0}" -eq 1 ] &&
			zxfer_check_literal_token_string "${g_cmd_decompress:-}"; then
			zxfer_split_tokens_into_result "${g_cmd_decompress:-}"
			l_scope_candidates="$l_scope_candidates ${g_zxfer_split_tokens_result%%"$ZXFER_LF"*}"
		fi
	fi
	zxfer_split_begin
	# shellcheck disable=SC2086 # Space-separated tool names.
	set -- $l_scope_candidates
	zxfer_split_end

	g_zxfer_remote_capability_requested_tools_result=zfs
	for l_scope_candidate in "$@"; do
		case " $g_zxfer_remote_capability_requested_tools_result " in
		*" $l_scope_candidate "*) ;;
		*) g_zxfer_remote_capability_requested_tools_result="$g_zxfer_remote_capability_requested_tools_result $l_scope_candidate" ;;
		esac
	done
	[ -n "$l_scope_tool" ] || return 0
	case " $g_zxfer_remote_capability_requested_tools_result " in
	*" $l_scope_tool "*) ;;
	*) g_zxfer_remote_capability_requested_tools_result="zfs $l_scope_tool" ;;
	esac
}

################################################################################
# CAPABILITY PROBE AND PER-ROLE CACHE
################################################################################

# Purpose: Look up one tool's record in the active capability channel.
# Usage: zxfer_get_parsed_remote_capability_tool_record TOOL; publishes
# g_zxfer_remote_capability_tool_status_result and
# g_zxfer_remote_capability_tool_path_result (empty unless the status is 0),
# or returns 1 when TOOL has no record.
zxfer_get_parsed_remote_capability_tool_record() {
	g_zxfer_remote_capability_tool_status_result=""
	g_zxfer_remote_capability_tool_path_result=""
	[ -n "$1" ] || return 1

	l_tool_record=$ZXFER_LF${g_zxfer_remote_capability_tool_records:-}$ZXFER_LF
	case $l_tool_record in
	*"$ZXFER_LF$1$ZXFER_TAB"*) ;;
	*) return 1 ;;
	esac
	l_tool_record=${l_tool_record#*"$ZXFER_LF$1$ZXFER_TAB"}
	l_tool_record=${l_tool_record%%"$ZXFER_LF"*}
	case $l_tool_record in
	*"$ZXFER_TAB"*) ;;
	*) return 1 ;;
	esac
	g_zxfer_remote_capability_tool_status_result=${l_tool_record%%"$ZXFER_TAB"*}
	g_zxfer_remote_capability_tool_path_result=${l_tool_record#*"$ZXFER_TAB"}
}

# Purpose: Parse and validate one capability response into the active channel.
# Usage: zxfer_parse_remote_capability_response RESPONSE [TOOLS]; publishes
# g_zxfer_remote_capability_os, _zfs_status and _tool_records (one
# "TOOL<TAB>STATUS<TAB>PATH" line per tool, with PATH validated, or empty when
# STATUS is not 0). Returns 1 unless RESPONSE is the V2 header, a non-empty os
# line, well-formed tool records that name each tool once and include zfs and
# each of the space-separated TOOLS, then a final "end" line.
zxfer_parse_remote_capability_response() {
	g_zxfer_remote_capability_os=""
	g_zxfer_remote_capability_zfs_status=""
	g_zxfer_remote_capability_tool_records=""

	# Allow one trailing newline, then walk the lines.
	l_parse_rest=${1%"$ZXFER_LF"}$ZXFER_LF
	l_parse_line_number=0
	l_parse_end_seen=0
	while [ -n "$l_parse_rest" ]; do
		l_parse_line=${l_parse_rest%%"$ZXFER_LF"*}
		l_parse_rest=${l_parse_rest#*"$ZXFER_LF"}
		l_parse_line_number=$((l_parse_line_number + 1))
		case $l_parse_line_number:$l_parse_line in
		1:ZXFER_REMOTE_CAPS_V2) continue ;;
		2:os"$ZXFER_TAB"?*)
			g_zxfer_remote_capability_os=${l_parse_line#os"$ZXFER_TAB"}
			continue
			;;
		1:* | 2:*) return 1 ;;
		esac
		# Nothing may follow the end line.
		[ "$l_parse_end_seen" -eq 0 ] || return 1
		if [ "$l_parse_line" = end ]; then
			l_parse_end_seen=1
			continue
		fi

		# A record is exactly "tool<TAB>NAME<TAB>STATUS<TAB>PATH", where PATH
		# is "-" unless STATUS is 0.
		case $l_parse_line in
		tool"$ZXFER_TAB"*"$ZXFER_TAB"*"$ZXFER_TAB"*) ;;
		*) return 1 ;;
		esac
		l_parse_fields=${l_parse_line#tool"$ZXFER_TAB"}
		l_parse_tool=${l_parse_fields%%"$ZXFER_TAB"*}
		l_parse_fields=${l_parse_fields#*"$ZXFER_TAB"}
		l_parse_status=${l_parse_fields%%"$ZXFER_TAB"*}
		l_parse_path=${l_parse_fields#*"$ZXFER_TAB"}
		[ -n "$l_parse_tool" ] || return 1
		zxfer_value_is_single_line "$l_parse_tool" || return 1
		zxfer_is_uint "$l_parse_status" || return 1
		# A tab left in PATH means a fifth field.
		zxfer_value_is_single_line "$l_parse_path" || return 1
		if [ "$l_parse_status" -eq 0 ]; then
			[ "$l_parse_path" != "-" ] || return 1
			zxfer_validate_resolved_tool_path "$l_parse_path" "$l_parse_tool" ||
				return 1
			l_parse_path=$g_zxfer_required_tool_result
		else
			[ "$l_parse_path" = "-" ] || return 1
			l_parse_path=""
		fi
		# Each tool may appear only once.
		! zxfer_get_parsed_remote_capability_tool_record "$l_parse_tool" || return 1
		g_zxfer_remote_capability_tool_records=${g_zxfer_remote_capability_tool_records:+$g_zxfer_remote_capability_tool_records$ZXFER_LF}$l_parse_tool$ZXFER_TAB$l_parse_status$ZXFER_TAB$l_parse_path
		[ "$l_parse_tool" != zfs ] || g_zxfer_remote_capability_zfs_status=$l_parse_status
	done
	[ "$l_parse_end_seen" -eq 1 ] || return 1
	[ -n "$g_zxfer_remote_capability_zfs_status" ] || return 1

	# Every requested tool needs a record, so a truncated response fails.
	zxfer_split_begin
	# shellcheck disable=SC2086 # Space-separated tool names.
	set -- ${2:-}
	zxfer_split_end
	for l_parse_tool in "$@"; do
		zxfer_get_parsed_remote_capability_tool_record "$l_parse_tool" || return 1
	done
}

# Purpose: Render the capability probe script for a tool list.
# Usage: zxfer_build_remote_capability_probe_script TOOLS; prints the readable
# script, one command per line and each ending in ";", and publishes its
# transport form in g_zxfer_remote_capability_probe_script_result: the
# nonblank lines joined by spaces, so a csh login shell gets one physical
# line. TOOLS is space-separated. Returns 1 when the secure PATH holds a
# control byte, so joining lines can never rewrite trusted configuration.
zxfer_build_remote_capability_probe_script() {
	g_zxfer_remote_capability_probe_script_result=""
	l_probe_path=${g_zxfer_secure_path:-$ZXFER_DEFAULT_SECURE_PATH}
	zxfer_value_is_single_line "$l_probe_path" || return 1
	zxfer_escape_single_quotes_into_result "$l_probe_path"
	l_probe_path=$g_zxfer_escaped_single_quotes_result

	zxfer_split_begin
	# shellcheck disable=SC2086 # Space-separated tool names.
	set -- ${1:-zfs}
	zxfer_split_end
	l_probe_tools=""
	for l_probe_tool in "$@"; do
		zxfer_escape_single_quotes_into_result "$l_probe_tool"
		l_probe_tools="$l_probe_tools${l_probe_tools:+ }'$g_zxfer_escaped_single_quotes_result'"
	done

	while IFS= read -r l_probe_line; do
		printf '%s\n' "$l_probe_line"
		[ -n "$l_probe_line" ] || continue
		g_zxfer_remote_capability_probe_script_result=$g_zxfer_remote_capability_probe_script_result${g_zxfer_remote_capability_probe_script_result:+ }$l_probe_line
	done <<-EOF
		PATH='$l_probe_path';
		export PATH;

		l_os=\$(uname 2>/dev/null) || exit \$?;
		printf '%s\n' 'ZXFER_REMOTE_CAPS_V2';
		printf '%s\t%s\n' 'os' "\$l_os";

		for l_tool in $l_probe_tools; do
		  [ -n "\$l_tool" ] || continue;
		  l_path=\$(command -v "\$l_tool" 2>/dev/null);
		  l_status=\$?;
		  if [ "\$l_status" -eq 0 ]; then
		    printf '%s\t%s\t0\t%s\n' 'tool' "\$l_tool" "\$l_path";
		  elif [ "\$l_status" -eq 1 ]; then
		    printf '%s\t%s\t1\t-\n' 'tool' "\$l_tool";
		  else
		    printf '%s\t%s\t%s\t-\n' 'tool' "\$l_tool" "\$l_status";
		  fi;
		done;

		printf '%s\n' 'end';
	EOF
}

# Purpose: Run one remote probe command and capture its output in this shell.
# Usage: zxfer_capture_remote_probe_output HOST_SPEC REMOTE_CMD [PROFILE_SIDE];
# returns the remote status and publishes g_zxfer_remote_probe_stdout
# (trailing newlines dropped) and g_zxfer_remote_probe_stderr. When the staged
# stderr cannot be read it drops stdout, sets
# g_zxfer_remote_probe_capture_failed=1 and returns the read status. The ssh
# argv becomes the failure report's last command. An invalid ssh policy or
# host spec, or a missing ssh, throws.
zxfer_capture_remote_probe_output() {
	g_zxfer_remote_probe_stdout=""
	g_zxfer_remote_probe_stderr=""
	g_zxfer_remote_probe_capture_failed=0
	# ssh runs in the substitution's subshell below, so count it here. Record
	# mode prepares the transport and renders the argv in this shell: a bad
	# policy or host spec throws here, where the report reaches the operator,
	# not in the subshell with stderr redirected, and the argv becomes the
	# report's last command.
	zxfer_profile_record_ssh_invocation "$1" "${3:-}"
	zxfer_ssh_shell_command_for_host record "$1" "$2" "${3:-}" || return
	zxfer_get_temp_file
	l_probe_stderr_file=$g_zxfer_temp_file_result
	if [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
		zxfer_echoV "Running remote probe [$(zxfer_get_remote_command_context_label "$1" "${3:-}")]: $2"
	fi
	l_probe_status=0
	g_zxfer_remote_probe_stdout=$(zxfer_invoke_ssh_shell_command_for_host \
		"$1" "$2" "${3:-}" 2>|"$l_probe_stderr_file") || l_probe_status=$?

	# The run root owns the file; an empty capture needs no read.
	[ -s "$l_probe_stderr_file" ] || return "$l_probe_status"
	zxfer_read_runtime_artifact_file "$l_probe_stderr_file" || {
		l_probe_status=$?
		g_zxfer_remote_probe_capture_failed=1
		g_zxfer_remote_probe_stdout=""
		g_zxfer_remote_probe_stderr="Failed to read remote probe stderr capture from local staging."
		return "$l_probe_status"
	}
	g_zxfer_remote_probe_stderr=$g_zxfer_runtime_artifact_read_result
	return "$l_probe_status"
}

# Purpose: Print the last remote probe's stderr, or a default message.
# Usage: zxfer_emit_remote_probe_failure_message [DEFAULT_MESSAGE]; prints
# nothing when both are empty.
zxfer_emit_remote_probe_failure_message() {
	l_default_message=${1:-}

	if [ -n "${g_zxfer_remote_probe_stderr:-}" ]; then
		printf '%s\n' "$g_zxfer_remote_probe_stderr"
		return 0
	fi
	[ -z "$l_default_message" ] || printf '%s\n' "$l_default_message"
}

# Purpose: Run a one-line script on a host under the secure PATH.
# Usage: zxfer_run_remote_probe_script HOST_SPEC PROFILE_SIDE SCRIPT; returns
# the status and results of zxfer_capture_remote_probe_output.
zxfer_run_remote_probe_script() {
	zxfer_escape_single_quotes_into_result "${g_zxfer_secure_path:-$ZXFER_DEFAULT_SECURE_PATH}"
	zxfer_build_remote_sh_c_command \
		"PATH='$g_zxfer_escaped_single_quotes_result'; export PATH; $3" >/dev/null
	zxfer_capture_remote_probe_output "$1" "$g_zxfer_remote_sh_c_command_result" "$2"
}

# Purpose: Probe a host live for its capabilities and parse the answer.
# Usage: zxfer_fetch_remote_host_capabilities_live HOST_SPEC PROFILE_SIDE TOOLS;
# publishes the parsed channel and g_zxfer_remote_capability_response_result.
# A failed probe prints its stderr and returns 1; a malformed response returns
# 1 silently.
zxfer_fetch_remote_host_capabilities_live() {
	g_zxfer_remote_capability_response_result=""
	zxfer_build_remote_capability_probe_script "$3" >/dev/null || return 1
	zxfer_build_remote_sh_c_command \
		"$g_zxfer_remote_capability_probe_script_result" >/dev/null
	if ! zxfer_capture_remote_probe_output "$1" "$g_zxfer_remote_sh_c_command_result" "$2"; then
		zxfer_emit_remote_probe_failure_message >&2
		return 1
	fi
	zxfer_parse_remote_capability_response "$g_zxfer_remote_probe_stdout" "$3" ||
		return 1
	g_zxfer_remote_capability_response_result=$g_zxfer_remote_probe_stdout
}

# Purpose: Load one role's capability slot into the active channel when it
# holds a validated response for HOST and TOOLS.
# Usage: zxfer_load_remote_capability_slot origin|target HOST TOOLS; returns 1,
# leaving the channel alone, on a miss.
zxfer_load_remote_capability_slot() {
	if [ "$1" = origin ]; then
		[ "${g_origin_remote_capabilities_host:-}" = "$2" ] &&
			[ "${g_origin_remote_capabilities_tools:-}" = "$3" ] &&
			[ -n "${g_origin_remote_capabilities_os:-}" ] || return 1
		g_zxfer_remote_capability_response_result=$g_origin_remote_capabilities_response
		g_zxfer_remote_capability_os=$g_origin_remote_capabilities_os
		g_zxfer_remote_capability_zfs_status=$g_origin_remote_capabilities_zfs_status
		g_zxfer_remote_capability_tool_records=$g_origin_remote_capabilities_tool_records
		return 0
	fi
	[ "${g_target_remote_capabilities_host:-}" = "$2" ] &&
		[ "${g_target_remote_capabilities_tools:-}" = "$3" ] &&
		[ -n "${g_target_remote_capabilities_os:-}" ] || return 1
	g_zxfer_remote_capability_response_result=$g_target_remote_capabilities_response
	g_zxfer_remote_capability_os=$g_target_remote_capabilities_os
	g_zxfer_remote_capability_zfs_status=$g_target_remote_capabilities_zfs_status
	g_zxfer_remote_capability_tool_records=$g_target_remote_capabilities_tool_records
}

# Purpose: Load a host's capabilities into the active channel, probing the host
# at most once per host and tool scope.
# Usage: zxfer_ensure_remote_host_capabilities HOST_SPEC [source|destination]
# [TOOL]; without a side the host's -O/-T role picks the slot, and the scope
# comes from zxfer_get_remote_capability_requested_tools_for_host. Prints the
# response and publishes it in g_zxfer_remote_capability_response_result, or
# returns non-zero with the channel empty when the probe or its validation
# fails, so callers probe directly.
zxfer_ensure_remote_host_capabilities() {
	l_caps_host=$1
	l_caps_side=${2:-}
	g_zxfer_remote_capability_response_result=""
	g_zxfer_remote_capability_os=""
	g_zxfer_remote_capability_zfs_status=""
	g_zxfer_remote_capability_tool_records=""
	[ -n "$l_caps_host" ] || return 1
	case $l_caps_side in
	source) l_caps_role=origin l_caps_other_role=target ;;
	destination) l_caps_role=target l_caps_other_role=origin ;;
	'')
		l_caps_role=origin l_caps_other_role=target
		[ "$l_caps_host" != "${g_option_T_target_host:-}" ] ||
			l_caps_role=target l_caps_other_role=origin
		;;
	*) return 1 ;;
	esac
	zxfer_get_remote_capability_requested_tools_for_host "$l_caps_host" "${3:-}"
	l_caps_tools=$g_zxfer_remote_capability_requested_tools_result

	# Only a validated response fills a slot, so a slot with an os is whole.
	# The probe depends only on the host spec and the tool scope, so the other
	# role's slot answers too: when -O and -T name one host (and so ask the
	# same scope, the union of both roles'), that host is probed once.
	if zxfer_load_remote_capability_slot "$l_caps_role" "$l_caps_host" "$l_caps_tools" ||
		zxfer_load_remote_capability_slot "$l_caps_other_role" "$l_caps_host" "$l_caps_tools"; then
		g_zxfer_profile_remote_capability_bootstrap_memory=$((g_zxfer_profile_remote_capability_bootstrap_memory + 1))
		printf '%s\n' "$g_zxfer_remote_capability_response_result"
		return 0
	fi

	zxfer_fetch_remote_host_capabilities_live "$l_caps_host" "$l_caps_side" \
		"$l_caps_tools" || return
	if [ "$l_caps_role" = origin ]; then
		g_origin_remote_capabilities_host=$l_caps_host
		g_origin_remote_capabilities_tools=$l_caps_tools
		g_origin_remote_capabilities_response=$g_zxfer_remote_capability_response_result
		g_origin_remote_capabilities_os=$g_zxfer_remote_capability_os
		g_origin_remote_capabilities_zfs_status=$g_zxfer_remote_capability_zfs_status
		g_origin_remote_capabilities_tool_records=$g_zxfer_remote_capability_tool_records
	else
		g_target_remote_capabilities_host=$l_caps_host
		g_target_remote_capabilities_tools=$l_caps_tools
		g_target_remote_capabilities_response=$g_zxfer_remote_capability_response_result
		g_target_remote_capabilities_os=$g_zxfer_remote_capability_os
		g_target_remote_capabilities_zfs_status=$g_zxfer_remote_capability_zfs_status
		g_target_remote_capabilities_tool_records=$g_zxfer_remote_capability_tool_records
	fi
	g_zxfer_profile_remote_capability_bootstrap_live=$((g_zxfer_profile_remote_capability_bootstrap_live + 1))
	printf '%s\n' "$g_zxfer_remote_capability_response_result"
}

# Purpose: Probe a configured -O or -T host's capabilities during startup.
# Usage: zxfer_preload_remote_host_capabilities HOST_SPEC source|destination;
# probe stderr reaches the operator only under -v or -V.
zxfer_preload_remote_host_capabilities() {
	if [ "${g_option_v_verbose:-0}" -eq 1 ] || [ "${g_option_V_very_verbose:-0}" -eq 1 ]; then
		zxfer_ensure_remote_host_capabilities "$1" "${2:-}" >/dev/null
		return
	fi
	zxfer_ensure_remote_host_capabilities "$1" "${2:-}" >/dev/null 2>&1
}

################################################################################
# REMOTE OS AND TOOL RESOLUTION
################################################################################

# Purpose: Find the operating system of the local host or of a remote host.
# Usage: zxfer_get_os HOST_SPEC [PROFILE_SIDE]; publishes it in
# g_zxfer_os_result and prints it. An empty HOST_SPEC runs local uname. A
# remote host answers from its capabilities, else from one direct uname probe;
# a failed probe prints its stderr and returns 1.
zxfer_get_os() {
	g_zxfer_os_result=""
	if [ -z "$1" ]; then
		g_zxfer_os_result=$(uname) || return
	elif zxfer_ensure_remote_host_capabilities "$1" "${2:-}" >/dev/null; then
		g_zxfer_os_result=$g_zxfer_remote_capability_os
	elif zxfer_run_remote_probe_script "$1" "${2:-}" "uname 2>/dev/null"; then
		g_zxfer_os_result=${g_zxfer_remote_probe_stdout%%"$ZXFER_LF"*}
		[ -n "$g_zxfer_os_result" ] || return 1
	else
		zxfer_emit_remote_probe_failure_message
		return 1
	fi
	printf '%s\n' "$g_zxfer_os_result"
}

# Purpose: Resolve one helper on a remote host to its absolute path.
# Usage: zxfer_resolve_remote_required_tool HOST_SPEC TOOL [LABEL]
# [PROFILE_SIDE]; publishes the path in g_zxfer_required_tool_result, or
# returns 1 with the operator message there. The capability record answers;
# a tool without one gets one direct probe.
zxfer_resolve_remote_required_tool() {
	l_tool_host=$1
	l_tool_name=$2
	l_tool_label=${3:-$2}
	l_tool_side=${4:-}
	g_zxfer_required_tool_result=""
	[ -n "$l_tool_host" ] || return 1

	if zxfer_ensure_remote_host_capabilities "$l_tool_host" "$l_tool_side" \
		"$l_tool_name" >/dev/null &&
		zxfer_get_parsed_remote_capability_tool_record "$l_tool_name"; then
		l_tool_status=$g_zxfer_remote_capability_tool_status_result
	else
		g_zxfer_profile_remote_cli_tool_direct_probes=$((g_zxfer_profile_remote_cli_tool_direct_probes + 1))
		zxfer_escape_single_quotes_into_result "$l_tool_name"
		zxfer_run_remote_probe_script "$l_tool_host" "$l_tool_side" \
			"l_path=\$(command -v '$g_zxfer_escaped_single_quotes_result' 2>/dev/null); l_status=\$?; if [ \"\$l_status\" -eq 0 ]; then printf '%s\n' \"\$l_path\"; elif [ \"\$l_status\" -eq 1 ]; then exit 10; else exit \"\$l_status\"; fi"
		case $? in
		0)
			zxfer_validate_resolved_tool_path "$g_zxfer_remote_probe_stdout" \
				"$l_tool_label" "host $l_tool_host"
			return
			;;
		10) l_tool_status=1 ;;
		*)
			g_zxfer_required_tool_result="Failed to query dependency \"$l_tool_label\" on host $l_tool_host."
			[ -z "${g_zxfer_remote_probe_stderr:-}" ] ||
				g_zxfer_required_tool_result=$g_zxfer_remote_probe_stderr
			return 1
			;;
		esac
	fi

	case $l_tool_status in
	0)
		g_zxfer_required_tool_result=$g_zxfer_remote_capability_tool_path_result
		;;
	1)
		g_zxfer_required_tool_result="Required dependency \"$l_tool_label\" not found on host $l_tool_host in secure PATH (${g_zxfer_secure_path:-$ZXFER_DEFAULT_SECURE_PATH}). Set ZXFER_SECURE_PATH/ZXFER_SECURE_PATH_APPEND for the remote host or install the binary."
		return 1
		;;
	*)
		g_zxfer_required_tool_result="Failed to query dependency \"$l_tool_label\" on host $l_tool_host."
		return 1
		;;
	esac
}
