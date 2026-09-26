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
# REMOTE DESTINATION SNAPSHOT BATCH DISCOVERY
################################################################################

# Module contract:
# owns globals: the destination discovery batch statuses
#   (g_zxfer_destination_discovery_batch_{inventory,pool,snapshot}_status and
#   g_zxfer_destination_discovery_batch_snapshot_ran), the rendered batch
#   script and remote command, and the last failure kind, status and
#   transport stderr.
# reads globals: g_option_T_target_host, g_destination, g_target_cmd_zfs,
#   g_cmd_zfs, g_zxfer_secure_path and g_cmd_awk.
# mutates caches: none.
# returns via stdout: the rendered batch script and the last transport stderr,
#   for $() callers; batch output streams into the caller's four files.

# Purpose: Clear the results of the last destination discovery batch.
# Usage: zxfer_reset_destination_discovery_batch_state; also called by the
# snapshot discovery reset.
zxfer_reset_destination_discovery_batch_state() {
	g_zxfer_destination_discovery_batch_inventory_status=""
	g_zxfer_destination_discovery_batch_pool_status=""
	g_zxfer_destination_discovery_batch_snapshot_status=""
	g_zxfer_destination_discovery_batch_snapshot_ran=""
	g_zxfer_remote_snapshot_discovery_batch_script_result=""
	g_zxfer_remote_destination_discovery_command_result=""
	g_zxfer_remote_destination_discovery_transport_stderr_result=""
	g_zxfer_remote_destination_discovery_failure_kind=""
	g_zxfer_remote_destination_discovery_failure_status=""
}

# Purpose: Render the target-side script that lists the destination inventory
# and snapshots, and probes the pool when the root is missing, in one ssh
# round trip.
# Usage: zxfer_build_remote_destination_discovery_batch_script ROOT_DATASET
# SNAPSHOT_DATASET POOL; publishes and prints
# g_zxfer_remote_snapshot_discovery_batch_script_result, a POSIX sh script.
# The script keeps its side files in one private `mktemp -d` workspace and
# removes it on every exit.
zxfer_build_remote_destination_discovery_batch_script() {
	zxfer_escape_single_quotes_into_result \
		"${g_zxfer_secure_path:-$ZXFER_DEFAULT_SECURE_PATH}"
	l_batch_path=$g_zxfer_escaped_single_quotes_result
	zxfer_escape_single_quotes_into_result "${g_target_cmd_zfs:-$g_cmd_zfs}"
	l_batch_zfs=$g_zxfer_escaped_single_quotes_result
	zxfer_escape_single_quotes_into_result "$1"
	l_batch_root=$g_zxfer_escaped_single_quotes_result
	zxfer_escape_single_quotes_into_result "$2"
	l_batch_snapshot=$g_zxfer_escaped_single_quotes_result
	zxfer_escape_single_quotes_into_result "$3"
	l_batch_pool=$g_zxfer_escaped_single_quotes_result

	# Read the script line by line in the current shell, which avoids a fork.
	# <<- strips the leading tabs, so the script reaches the target unindented.
	l_batch_script=""
	while IFS= read -r l_batch_line; do
		l_batch_script=$l_batch_script$l_batch_line$ZXFER_LF
	done <<-EOF
		PATH='$l_batch_path'
		export PATH

		l_zfs_cmd='$l_batch_zfs'
		l_destination_root_dataset='$l_batch_root'
		l_destination_snapshot_dataset='$l_batch_snapshot'
		l_destination_pool='$l_batch_pool'

		zxfer_cleanup_destination_discovery_batch() {
			if [ "\$l_inventory_pid" != "" ]; then
				kill "\$l_inventory_pid" 2>/dev/null || :
				wait "\$l_inventory_pid" 2>/dev/null || :
			fi
			if [ "\$l_workspace" != "" ]; then
				rm -rf "\$l_workspace" 2>/dev/null || :
			fi
		}

		zxfer_emit_destination_discovery_section_file() {
			l_section_name=\$1
			l_section_file=\$2

			printf '%s\t%s\n' 'BEGIN' "\$l_section_name"
			if [ -s "\$l_section_file" ]; then
				cat "\$l_section_file" || return \$?
			fi
			printf '%s\t%s\n' 'END' "\$l_section_name"
		}

		zxfer_destination_discovery_stderr_reports_missing() {
			l_stderr_file=\$1
			grep -F \
				-e 'dataset does not exist' \
				-e 'Dataset does not exist' \
				-e 'no such dataset' \
				-e 'No such dataset' \
				-e 'no such pool or dataset' \
				-e 'No such pool or dataset' \
				"\$l_stderr_file" >/dev/null 2>&1
		}

		l_workspace=''
		l_inventory_pid=''
		trap 'zxfer_cleanup_destination_discovery_batch' 0
		trap 'zxfer_cleanup_destination_discovery_batch; exit 1' HUP INT TERM QUIT

		l_tmpdir=\${TMPDIR:-/tmp}
		case "\$l_tmpdir" in
		/*)
			:
			;;
		*)
			l_tmpdir=/tmp
			;;
		esac
		umask 077
		l_workspace=\$(mktemp -d "\$l_tmpdir/zxfer.destination-discovery.XXXXXX" 2>/dev/null) || exit \$?
		case "\$l_workspace" in
		/?*)
			:
			;;
		*)
			exit 1
			;;
		esac
		l_inventory_stdout_file=\$l_workspace/inventory
		l_inventory_stderr_file=\$l_workspace/inventory-stderr
		l_pool_stderr_file=\$l_workspace/pool-stderr
		l_snapshot_stderr_file=\$l_workspace/snapshot-stderr

		"\$l_zfs_cmd" list -t filesystem,volume -Hr -o name "\$l_destination_root_dataset" >"\$l_inventory_stdout_file" 2>"\$l_inventory_stderr_file" &
		l_inventory_pid=\$!
		l_pool_status=''
		l_snapshot_status=0
		l_snapshot_ran=1

		printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_V1'
		printf '%s\t%s\n' 'BEGIN' 'snapshot_stdout'
		"\$l_zfs_cmd" list -Hr -o name,guid -t snapshot "\$l_destination_snapshot_dataset" 2>"\$l_snapshot_stderr_file"
		l_snapshot_status=\$?
		printf '%s\t%s\n' 'END' 'snapshot_stdout'

		l_inventory_status=0
		wait "\$l_inventory_pid" || l_inventory_status=\$?
		l_inventory_pid=''

		if [ "\$l_inventory_status" -ne 0 ]; then
			if zxfer_destination_discovery_stderr_reports_missing "\$l_inventory_stderr_file"; then
				"\$l_zfs_cmd" list -H -o name "\$l_destination_pool" >/dev/null 2>"\$l_pool_stderr_file"
				l_pool_status=\$?
				if [ "\$l_pool_status" -eq 0 ] && zxfer_destination_discovery_stderr_reports_missing "\$l_snapshot_stderr_file"; then
					l_snapshot_status=0
					: >"\$l_snapshot_stderr_file"
				fi
			fi
		fi

		if [ "\$l_inventory_status" -eq 0 ]; then
			grep -F -x -e "\$l_destination_snapshot_dataset" "\$l_inventory_stdout_file" >/dev/null 2>&1
			l_grep_status=\$?
			case "\$l_grep_status" in
			0)
				:
				;;
			1)
				l_snapshot_status=0
				: >"\$l_snapshot_stderr_file"
				:
				;;
			*)
				l_inventory_status=\$l_grep_status
				printf 'Failed to scan destination dataset inventory for %s.\n' "\$l_destination_snapshot_dataset" >"\$l_inventory_stderr_file"
				;;
			esac
		fi

		printf '%s\t%s\t%s\n' 'STATUS' 'inventory' "\$l_inventory_status"
		printf '%s\t%s\t%s\n' 'STATUS' 'pool' "\$l_pool_status"
		printf '%s\t%s\t%s\n' 'STATUS' 'snapshot_ran' "\$l_snapshot_ran"
		zxfer_emit_destination_discovery_section_file inventory_stdout "\$l_inventory_stdout_file" || exit \$?
		zxfer_emit_destination_discovery_section_file inventory_stderr "\$l_inventory_stderr_file" || exit \$?
		zxfer_emit_destination_discovery_section_file pool_stderr "\$l_pool_stderr_file" || exit \$?
		printf '%s\t%s\t%s\n' 'STATUS' 'snapshot' "\$l_snapshot_status"
		zxfer_emit_destination_discovery_section_file snapshot_stderr "\$l_snapshot_stderr_file" || exit \$?
		printf '%s\n' 'ZXFER_DESTINATION_DISCOVERY_BATCH_END'
	EOF
	g_zxfer_remote_snapshot_discovery_batch_script_result=${l_batch_script%"$ZXFER_LF"}
	printf '%s\n' "$g_zxfer_remote_snapshot_discovery_batch_script_result"
}

# Purpose: Render the one remote discovery command and check the ssh policy
# and -T host spec before the streaming pipeline starts.
# Usage: zxfer_prepare_remote_destination_discovery_batch_command DATASET;
# publishes g_zxfer_remote_destination_discovery_command_result. Policy and
# host-spec failures throw here, in the parent shell, because a throw inside
# the pipeline would only end a subshell.
zxfer_prepare_remote_destination_discovery_batch_command() {
	zxfer_build_remote_destination_discovery_batch_script \
		"$g_destination" "$1" "${g_destination%%/*}" >/dev/null
	zxfer_build_remote_sh_c_command \
		"$g_zxfer_remote_snapshot_discovery_batch_script_result" >/dev/null
	if ! zxfer_prepare_ssh_transport; then
		zxfer_throw_error "$g_zxfer_ssh_transport_error"
		return 1
	fi
	zxfer_prepare_ssh_shell_command_context "$g_option_T_target_host" \
		"$g_zxfer_remote_sh_c_command_result" || {
		l_batch_context_status=$?
		[ -z "$g_zxfer_ssh_shell_context_error_result" ] ||
			zxfer_throw_error "$g_zxfer_ssh_shell_context_error_result"
		return "$l_batch_context_status"
	}
	g_zxfer_remote_destination_discovery_command_result=$g_zxfer_remote_sh_c_command_result
}

# Purpose: Run destination discovery on the -T host in one ssh round trip and
# stream its sections into the caller's four files.
# Usage: zxfer_run_remote_destination_discovery_batch_to_files DATASET
# INVENTORY_FILE INVENTORY_STDERR_FILE SNAPSHOT_FILE SNAPSHOT_STDERR_FILE;
# publishes the four batch statuses only on success. On any failure the
# statuses stay empty and all four files are emptied, so no partial listing
# survives; zxfer_report_remote_destination_discovery_failure reports it.
zxfer_run_remote_destination_discovery_batch_to_files() {
	zxfer_reset_destination_discovery_batch_state
	zxfer_prepare_remote_destination_discovery_batch_command "$1" || return
	shift
	# The parser appends, and a section with no lines never opens its file.
	for l_batch_file in "$@"; do
		printf '' >"$l_batch_file" || return
	done
	zxfer_get_temp_file || return
	l_batch_status_file=$g_zxfer_temp_file_result
	zxfer_get_temp_file || return
	l_batch_stderr_file=$g_zxfer_temp_file_result

	# POSIX sh has no pipefail, so the ssh status crosses the pipe in the
	# status file. The parser appends its status line only after the left
	# side has written that status and exited.
	zxfer_echoV "Running remote destination discovery batch for $g_destination."
	l_batch_parser_status=0
	# shellcheck disable=SC2016 # The awk program reads $0 itself.
	{
		l_batch_transport_status=0
		zxfer_invoke_ssh_shell_command_for_host "$g_option_T_target_host" \
			"$g_zxfer_remote_destination_discovery_command_result" destination \
			2>"$l_batch_stderr_file" || l_batch_transport_status=$?
		printf '%s\n' "$l_batch_transport_status" >"$l_batch_status_file"
	} | "${g_cmd_awk:-awk}" -v inventory_out="$1" -v inventory_err="$2" \
		-v snapshot_out="$3" -v snapshot_err="$4" \
		-v status_out="$l_batch_status_file" '
		# The target writes a fixed sequence of records. protocol_step indexes
		# the next one: a header, the snapshot_stdout section, the inventory,
		# pool and snapshot_ran statuses, the inventory_stdout,
		# inventory_stderr and pool_stderr sections, the snapshot status, the
		# snapshot_stderr section and an end marker. Anything else, or a
		# record out of order, fails the batch.
		BEGIN {
			tab = sprintf("%c", 9)
			steps = split("header section:snapshot_stdout status:inventory " \
				"status:pool status:snapshot_ran section:inventory_stdout " \
				"section:inventory_stderr section:pool_stderr status:snapshot " \
				"section:snapshot_stderr end", order, " ")
			output["snapshot_stdout"] = snapshot_out
			output["inventory_stdout"] = inventory_out
			output["inventory_stderr"] = inventory_err
			output["snapshot_stderr"] = snapshot_err
			protocol_step = 1
			section = ""
		}
		bad { next }
		section != "" {
			if ($0 == "END" tab section) {
				section = ""
				protocol_step++
			} else if (output[section] != "") {
				print >> output[section]
			}
			next
		}
		protocol_step > steps {
			# Only blank lines may follow the end marker.
			if ($0 != "") bad = 1
			next
		}
		{
			split(order[protocol_step], want, ":")
			if ((want[1] == "header" && $0 == "ZXFER_DESTINATION_DISCOVERY_BATCH_V1") ||
				(want[1] == "end" && $0 == "ZXFER_DESTINATION_DISCOVERY_BATCH_END")) {
				protocol_step++
			} else if (want[1] == "section" && $0 == "BEGIN" tab want[2]) {
				section = want[2]
			} else if (want[1] == "status" && index($0, "STATUS" tab want[2] tab) == 1) {
				status[want[2]] = substr($0, length(want[2]) + 9)
				protocol_step++
			} else {
				bad = 1
			}
		}
		END {
			if (bad || section != "" || protocol_step <= steps) exit 1
			# Every status is a number; the pool status is empty when the
			# target skipped the pool probe.
			if (status["inventory"] !~ /^[0-9]+$/ || status["snapshot"] !~ /^[0-9]+$/ ||
				status["snapshot_ran"] !~ /^[0-9]+$/ || status["pool"] !~ /^[0-9]*$/) exit 1
			print status["inventory"], status["snapshot"], status["snapshot_ran"], \
				status["pool"] >> status_out
		}' || l_batch_parser_status=$?

	# Line 1 is the ssh status. Line 2 holds the inventory, snapshot,
	# snapshot_ran and pool statuses; the statuses are published only after a
	# clean transport and parse, so a failure leaves them empty.
	l_batch_transport_status=""
	l_batch_inventory_status=""
	l_batch_snapshot_status=""
	l_batch_snapshot_ran=""
	l_batch_pool_status=""
	{
		IFS= read -r l_batch_transport_status
		IFS=' ' read -r l_batch_inventory_status l_batch_snapshot_status \
			l_batch_snapshot_ran l_batch_pool_status
	} <"$l_batch_status_file"

	if ! zxfer_is_uint "$l_batch_transport_status"; then
		g_zxfer_remote_destination_discovery_failure_kind=transport_status_malformed
		g_zxfer_remote_destination_discovery_failure_status=1
	elif [ "$l_batch_transport_status" -ne 0 ]; then
		zxfer_read_runtime_artifact_file "$l_batch_stderr_file" || :
		g_zxfer_remote_destination_discovery_transport_stderr_result=$g_zxfer_runtime_artifact_read_result
		g_zxfer_remote_destination_discovery_failure_kind=transport
		g_zxfer_remote_destination_discovery_failure_status=$l_batch_transport_status
	elif [ "$l_batch_parser_status" -ne 0 ]; then
		g_zxfer_remote_destination_discovery_failure_kind=batch_parse
		g_zxfer_remote_destination_discovery_failure_status=$l_batch_parser_status
	elif ! zxfer_is_uint "$l_batch_inventory_status" ||
		! zxfer_is_uint "$l_batch_snapshot_status" ||
		! zxfer_is_uint "$l_batch_snapshot_ran" ||
		{ [ -n "$l_batch_pool_status" ] && ! zxfer_is_uint "$l_batch_pool_status"; }; then
		# awk exited 0 without writing a valid status line.
		g_zxfer_remote_destination_discovery_failure_kind=batch_parse
		g_zxfer_remote_destination_discovery_failure_status=1
	else
		g_zxfer_destination_discovery_batch_inventory_status=$l_batch_inventory_status
		g_zxfer_destination_discovery_batch_snapshot_status=$l_batch_snapshot_status
		g_zxfer_destination_discovery_batch_snapshot_ran=$l_batch_snapshot_ran
		g_zxfer_destination_discovery_batch_pool_status=$l_batch_pool_status
		zxfer_profile_record_zfs_call destination list
		[ -z "$l_batch_pool_status" ] ||
			zxfer_profile_record_zfs_call destination list
		[ "$l_batch_snapshot_ran" -ne 1 ] ||
			zxfer_profile_record_zfs_call destination list
	fi
	zxfer_cleanup_runtime_artifact_path_list \
		"$l_batch_status_file$ZXFER_LF$l_batch_stderr_file" || :
	[ -n "$g_zxfer_remote_destination_discovery_failure_kind" ] || return 0

	# Fail closed: leave no partial listing behind.
	for l_batch_file in "$@"; do
		printf '' >"$l_batch_file" || :
	done
	zxfer_report_remote_destination_discovery_failure
}

# Purpose: Report the last batch failure and return its status.
# Usage: zxfer_report_remote_destination_discovery_failure. An ssh failure
# only returns its status (the caller reports it with the transport stderr);
# a malformed transport status or batch response throws.
zxfer_report_remote_destination_discovery_failure() {
	case $g_zxfer_remote_destination_discovery_failure_kind in
	transport_status_malformed)
		zxfer_throw_error "Malformed destination discovery transport status."
		return 1
		;;
	batch_parse)
		zxfer_throw_error "Malformed destination discovery batch response." \
			"$g_zxfer_remote_destination_discovery_failure_status"
		;;
	esac
	return "${g_zxfer_remote_destination_discovery_failure_status:-1}"
}

# Purpose: Print the ssh stderr of the last failed batch.
# Usage: zxfer_get_remote_destination_discovery_transport_stderr; prints
# nothing unless the last failure was an ssh failure.
zxfer_get_remote_destination_discovery_transport_stderr() {
	printf '%s' "${g_zxfer_remote_destination_discovery_transport_stderr_result:-}"
}

# Purpose: Tell whether the last batch failed in ssh itself.
# Usage: zxfer_remote_destination_discovery_failure_is_transport.
zxfer_remote_destination_discovery_failure_is_transport() {
	[ "${g_zxfer_remote_destination_discovery_failure_kind:-}" = transport ]
}
