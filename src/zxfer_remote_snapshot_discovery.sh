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
# owns globals: destination batch status/result channels for inventory, pool,
#   snapshot, and snapshot-ran state; the contained local operation workspace;
#   in-shell script, parser, transport, and failure result channels.
# reads globals: target-host execution context, resolved remote helpers,
#   destination roots, compression settings, and producer staging helpers.
# mutates caches: none.
# returns via stdout: rendered remote batch scripts and validated transport
#   stderr; transactionally publishes separated inventory, stderr, and snapshot
#   streams only after the complete transport protocol passes validation.

# Purpose: Return whether a status value from the destination discovery batch
# is numeric.
# Usage: Called while parsing target-side destination discovery output before
# zxfer trusts a remote command status for local failure handling.
zxfer_destination_discovery_batch_status_is_numeric() {
	case "${1:-}" in
	'' | *[!0-9]*)
		return 1
		;;
	esac

	return 0
}

# Purpose: Reset the remote destination discovery batch scratch state.
# Usage: Called before parsing a target-side batch payload so stale statuses
# cannot leak into the current discovery result.
zxfer_reset_destination_discovery_batch_state() {
	zxfer_reset_destination_discovery_batch_status_state
	zxfer_reset_remote_destination_discovery_workspace_state
	g_zxfer_remote_destination_discovery_command_result=""
	g_zxfer_remote_destination_discovery_parser_status_result=""
	g_zxfer_remote_destination_discovery_transport_status_result=""
	g_zxfer_remote_destination_discovery_transport_stderr_result=""
	g_zxfer_remote_destination_discovery_failure_kind=""
	g_zxfer_remote_destination_discovery_failure_status=""
	zxfer_reset_remote_destination_discovery_batch_script_state
	zxfer_reset_remote_destination_discovery_batch_parser_state
}

# Purpose: Reset only the validated destination discovery status channel.
# Usage: The status-sidecar loader calls this without discarding the command,
# parser, or workspace state still owned by the active operation.
zxfer_reset_destination_discovery_batch_status_state() {
	g_zxfer_destination_discovery_batch_inventory_status=""
	g_zxfer_destination_discovery_batch_pool_status=""
	g_zxfer_destination_discovery_batch_snapshot_status=""
	g_zxfer_destination_discovery_batch_snapshot_ran=""
	g_zxfer_destination_discovery_batch_inventory_status_seen=0
	g_zxfer_destination_discovery_batch_pool_status_seen=0
	g_zxfer_destination_discovery_batch_snapshot_status_seen=0
	g_zxfer_destination_discovery_batch_snapshot_ran_status_seen=0
	g_zxfer_destination_discovery_batch_status_name_result=""
	g_zxfer_destination_discovery_batch_status_value_result=""
}

# Purpose: Reset the fixed paths for one local destination-discovery workspace.
# Usage: Called before allocating a private run-root child and after its single
# normal cleanup so stale descendants can never authorize a later publication.
zxfer_reset_remote_destination_discovery_workspace_state() {
	g_zxfer_remote_destination_discovery_workspace=""
	g_zxfer_remote_destination_discovery_rollback_dir=""
	g_zxfer_remote_destination_discovery_transport_status_file=""
	g_zxfer_remote_destination_discovery_transport_stderr_file=""
	g_zxfer_remote_destination_discovery_batch_status_file=""
	g_zxfer_remote_destination_discovery_inventory_stage_file=""
	g_zxfer_remote_destination_discovery_inventory_stderr_stage_file=""
	g_zxfer_remote_destination_discovery_snapshot_stage_file=""
	g_zxfer_remote_destination_discovery_snapshot_stderr_stage_file=""
	g_zxfer_remote_destination_discovery_inventory_target_file=""
	g_zxfer_remote_destination_discovery_inventory_stderr_target_file=""
	g_zxfer_remote_destination_discovery_snapshot_target_file=""
	g_zxfer_remote_destination_discovery_snapshot_stderr_target_file=""
}

# Purpose: Reset the in-shell result channel used to render one remote
# destination discovery batch script.
# Usage: Called before preparing renderer inputs so no prior script fragment
# can leak into the next target command.
zxfer_reset_remote_destination_discovery_batch_script_state() {
	g_zxfer_remote_snapshot_discovery_batch_script_result=""
	g_zxfer_remote_snapshot_discovery_batch_script_destination_root_single=""
	g_zxfer_remote_snapshot_discovery_batch_script_destination_snapshot_single=""
	g_zxfer_remote_snapshot_discovery_batch_script_destination_pool_single=""
	g_zxfer_remote_snapshot_discovery_batch_script_zfs_cmd_single=""
	g_zxfer_remote_snapshot_discovery_batch_script_dependency_path_single=""
}

# Purpose: Append stdin lines to the current remote batch script result.
# Usage: Renderer stages pass here-doc fragments directly so composition stays
# in the current shell and does not add command substitutions or helper
# processes.
zxfer_append_remote_destination_discovery_batch_script_lines() {
	while IFS= read -r l_remote_destination_discovery_batch_script_line ||
		[ -n "$l_remote_destination_discovery_batch_script_line" ]; do
		if [ -n "$g_zxfer_remote_snapshot_discovery_batch_script_result" ]; then
			g_zxfer_remote_snapshot_discovery_batch_script_result=$g_zxfer_remote_snapshot_discovery_batch_script_result'
'$l_remote_destination_discovery_batch_script_line
		else
			g_zxfer_remote_snapshot_discovery_batch_script_result=$l_remote_destination_discovery_batch_script_line
		fi
	done
}

# Purpose: Prepare escaped values interpolated into the target-side script.
# Usage: Called once before the fixed renderer stages; preserves the original
# six command substitutions and their quoting boundaries.
zxfer_prepare_remote_destination_discovery_batch_script_inputs() {
	l_remote_destination_discovery_batch_script_root=$1
	l_remote_destination_discovery_batch_script_snapshot=$2
	l_remote_destination_discovery_batch_script_pool=$3
	l_remote_destination_discovery_batch_script_zfs_cmd=${g_target_cmd_zfs:-$g_cmd_zfs}

	g_zxfer_remote_snapshot_discovery_batch_script_destination_root_single=$(zxfer_escape_for_single_quotes "$l_remote_destination_discovery_batch_script_root")
	g_zxfer_remote_snapshot_discovery_batch_script_destination_snapshot_single=$(zxfer_escape_for_single_quotes "$l_remote_destination_discovery_batch_script_snapshot")
	g_zxfer_remote_snapshot_discovery_batch_script_destination_pool_single=$(zxfer_escape_for_single_quotes "$l_remote_destination_discovery_batch_script_pool")
	g_zxfer_remote_snapshot_discovery_batch_script_zfs_cmd_single=$(zxfer_escape_for_single_quotes "$l_remote_destination_discovery_batch_script_zfs_cmd")
	if l_remote_destination_discovery_batch_script_dependency_path=$(zxfer_get_effective_dependency_path); then
		:
	else
		l_remote_destination_discovery_batch_script_dependency_status=$?
		return "$l_remote_destination_discovery_batch_script_dependency_status"
	fi
	g_zxfer_remote_snapshot_discovery_batch_script_dependency_path_single=$(zxfer_escape_for_single_quotes "$l_remote_destination_discovery_batch_script_dependency_path")
}

# Purpose: Render target-side variables, cleanup, and section helpers.
# Usage: First fixed stage of the remote destination discovery script.
zxfer_render_remote_destination_discovery_batch_script_support_stage() {
	zxfer_append_remote_destination_discovery_batch_script_lines <<-EOF
		PATH='$g_zxfer_remote_snapshot_discovery_batch_script_dependency_path_single'
		export PATH

		l_zfs_cmd='$g_zxfer_remote_snapshot_discovery_batch_script_zfs_cmd_single'
		l_destination_root_dataset='$g_zxfer_remote_snapshot_discovery_batch_script_destination_root_single'
		l_destination_snapshot_dataset='$g_zxfer_remote_snapshot_discovery_batch_script_destination_snapshot_single'
		l_destination_pool='$g_zxfer_remote_snapshot_discovery_batch_script_destination_pool_single'

		zxfer_cleanup_destination_discovery_batch() {
			if [ "\$l_inventory_pid" != "" ]; then
				kill "\$l_inventory_pid" 2>/dev/null || :
				wait "\$l_inventory_pid" 2>/dev/null || :
			fi
			for l_cleanup_file in "\$l_inventory_stdout_file" "\$l_inventory_stderr_file" "\$l_pool_stderr_file" "\$l_snapshot_stderr_file"; do
				[ "\$l_cleanup_file" != "" ] || continue
				rm -f "\$l_cleanup_file" 2>/dev/null || :
			done
		}

		zxfer_emit_destination_discovery_section_file() {
			l_section_name=\$1
			l_section_file=\$2

			printf '%s\t%s\n' 'BEGIN' "\$l_section_name"
			if [ -f "\$l_section_file" ]; then
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

		l_inventory_stdout_file=''
		l_inventory_stderr_file=''
		l_pool_stderr_file=''
		l_snapshot_stderr_file=''
		l_inventory_pid=''
		trap 'zxfer_cleanup_destination_discovery_batch' 0
		trap 'zxfer_cleanup_destination_discovery_batch; exit 1' HUP INT TERM QUIT
	EOF
}

# Purpose: Render secure temp allocation for the target-side batch.
# Usage: Second fixed stage; preserves the remote TMPDIR and umask policy.
zxfer_render_remote_destination_discovery_batch_script_staging_stage() {
	zxfer_append_remote_destination_discovery_batch_script_lines <<-EOF

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
		l_inventory_stdout_file=\$(mktemp "\$l_tmpdir/zxfer.destination-discovery.inventory.XXXXXX" 2>/dev/null) || exit \$?
		l_inventory_stderr_file=\$(mktemp "\$l_tmpdir/zxfer.destination-discovery.inventory-stderr.XXXXXX" 2>/dev/null) || exit \$?
		l_pool_stderr_file=\$(mktemp "\$l_tmpdir/zxfer.destination-discovery.pool-stderr.XXXXXX" 2>/dev/null) || exit \$?
		l_snapshot_stderr_file=\$(mktemp "\$l_tmpdir/zxfer.destination-discovery.snapshots-stderr.XXXXXX" 2>/dev/null) || exit \$?
	EOF
}

# Purpose: Render concurrent inventory and snapshot discovery commands.
# Usage: Third fixed stage; preserves the one background inventory job and
# direct snapshot stdout stream.
zxfer_render_remote_destination_discovery_batch_script_discovery_stage() {
	zxfer_append_remote_destination_discovery_batch_script_lines <<-EOF

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
	EOF
}

# Purpose: Render missing-root and dataset-presence classification.
# Usage: Fourth fixed stage; preserves target-side status normalization and
# exact fixed-string inventory scanning.
zxfer_render_remote_destination_discovery_batch_script_classification_stage() {
	zxfer_append_remote_destination_discovery_batch_script_lines <<-EOF

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
	EOF
}

# Purpose: Render status and section publication for the remote batch.
# Usage: Final fixed stage; preserves section order and protocol sentinels.
zxfer_render_remote_destination_discovery_batch_script_publication_stage() {
	zxfer_append_remote_destination_discovery_batch_script_lines <<-EOF

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
}

# Purpose: Build the target-side destination discovery script for the next
# remote batch execution.
# Usage: Called by the remote destination discovery path so dataset inventory,
# missing-root pool probing, and snapshot listing share one SSH round trip while
# keeping the same portable ZFS command shapes.
# Returns: A POSIX sh script suitable for execution by sh -c on the target host.
zxfer_build_remote_destination_discovery_batch_script() {
	zxfer_reset_remote_destination_discovery_batch_script_state
	zxfer_prepare_remote_destination_discovery_batch_script_inputs \
		"$1" "$2" "$3" || return "$?"
	zxfer_render_remote_destination_discovery_batch_script_support_stage
	zxfer_render_remote_destination_discovery_batch_script_staging_stage
	zxfer_render_remote_destination_discovery_batch_script_discovery_stage
	zxfer_render_remote_destination_discovery_batch_script_classification_stage
	zxfer_render_remote_destination_discovery_batch_script_publication_stage

	cat <<-EOF
		$g_zxfer_remote_snapshot_discovery_batch_script_result
	EOF
}

# Purpose: Parse one compact status-sidecar row into owner-prefixed results.
# Usage: The loader calls this before role-specific duplicate checks; embedded
# tabs remain part of the value and are rejected by numeric validation later.
zxfer_parse_destination_discovery_batch_status_line() {
	l_batch_status_parse_line=$1
	l_batch_status_parse_tab='	'
	g_zxfer_destination_discovery_batch_status_name_result=""
	g_zxfer_destination_discovery_batch_status_value_result=""

	case "$l_batch_status_parse_line" in
	*"$l_batch_status_parse_tab"*)
		g_zxfer_destination_discovery_batch_status_name_result=${l_batch_status_parse_line%%"$l_batch_status_parse_tab"*}
		g_zxfer_destination_discovery_batch_status_value_result=${l_batch_status_parse_line#*"$l_batch_status_parse_tab"}
		;;
	*)
		return 1
		;;
	esac
}

# Purpose: Record one parsed sidecar row in the explicit status-owner slot.
# Usage: Called only after row framing succeeds; duplicate and unknown names
# fail closed before any complete status set can be accepted.
zxfer_record_destination_discovery_batch_status() {
	l_batch_status_record_name=$g_zxfer_destination_discovery_batch_status_name_result
	l_batch_status_record_value=$g_zxfer_destination_discovery_batch_status_value_result

	case "$l_batch_status_record_name" in
	inventory)
		[ "$g_zxfer_destination_discovery_batch_inventory_status_seen" -eq 0 ] || return 1
		g_zxfer_destination_discovery_batch_inventory_status=$l_batch_status_record_value
		g_zxfer_destination_discovery_batch_inventory_status_seen=1
		;;
	pool)
		[ "$g_zxfer_destination_discovery_batch_pool_status_seen" -eq 0 ] || return 1
		g_zxfer_destination_discovery_batch_pool_status=$l_batch_status_record_value
		g_zxfer_destination_discovery_batch_pool_status_seen=1
		;;
	snapshot)
		[ "$g_zxfer_destination_discovery_batch_snapshot_status_seen" -eq 0 ] || return 1
		g_zxfer_destination_discovery_batch_snapshot_status=$l_batch_status_record_value
		g_zxfer_destination_discovery_batch_snapshot_status_seen=1
		;;
	snapshot_ran)
		[ "$g_zxfer_destination_discovery_batch_snapshot_ran_status_seen" -eq 0 ] || return 1
		g_zxfer_destination_discovery_batch_snapshot_ran=$l_batch_status_record_value
		g_zxfer_destination_discovery_batch_snapshot_ran_status_seen=1
		;;
	*)
		return 1
		;;
	esac
}

# Purpose: Validate completeness and numeric shape of the loaded status set.
# Usage: Final sidecar stage; pool status alone may be empty when no fallback
# pool probe ran, matching the target protocol's existing contract.
zxfer_validate_loaded_destination_discovery_batch_statuses() {
	[ "$g_zxfer_destination_discovery_batch_inventory_status_seen" -eq 1 ] || return 1
	[ "$g_zxfer_destination_discovery_batch_pool_status_seen" -eq 1 ] || return 1
	[ "$g_zxfer_destination_discovery_batch_snapshot_status_seen" -eq 1 ] || return 1
	[ "$g_zxfer_destination_discovery_batch_snapshot_ran_status_seen" -eq 1 ] || return 1
	zxfer_destination_discovery_batch_status_is_numeric \
		"$g_zxfer_destination_discovery_batch_inventory_status" || return 1
	zxfer_destination_discovery_batch_status_is_numeric \
		"$g_zxfer_destination_discovery_batch_snapshot_status" || return 1
	zxfer_destination_discovery_batch_status_is_numeric \
		"$g_zxfer_destination_discovery_batch_snapshot_ran" || return 1
	[ -z "$g_zxfer_destination_discovery_batch_pool_status" ] ||
		zxfer_destination_discovery_batch_status_is_numeric \
			"$g_zxfer_destination_discovery_batch_pool_status"
}

# Purpose: Load the compact status sidecar from destination discovery parsing.
# Usage: Called after streamed sections are complete; line parsing, duplicate
# ownership, and final completeness are separate protocol stages.
zxfer_load_destination_discovery_batch_status_file() {
	l_batch_status_load_file=$1
	zxfer_reset_destination_discovery_batch_status_state

	while IFS= read -r l_batch_status_load_line ||
		[ -n "$l_batch_status_load_line" ]; do
		zxfer_parse_destination_discovery_batch_status_line \
			"$l_batch_status_load_line" || return 1
		zxfer_record_destination_discovery_batch_status || return 1
	done <"$l_batch_status_load_file"

	zxfer_validate_loaded_destination_discovery_batch_statuses
}

# Purpose: Reset the in-shell AWK parser result channel.
# Usage: Called before parser stages append their fixed program fragments.
zxfer_reset_remote_destination_discovery_batch_parser_state() {
	g_zxfer_remote_snapshot_discovery_batch_parser_program=""
}

# Purpose: Render parser status validation and fixed protocol-order checks.
# Usage: First AWK support stage; every status must appear once at its exact
# position between the streamed sections emitted by the target renderer.
zxfer_render_remote_destination_discovery_batch_parser_status_stage() {
	# shellcheck disable=SC2016  # awk program should see literal $0.
	g_zxfer_remote_snapshot_discovery_batch_parser_program='
		function fail() {
			bad = 1
		}
		function record_status(name, value) {
			if (name == "inventory") {
				if (seen_inventory_status != 0 || protocol_step != 2) {
					fail()
				}
				inventory_status = value
				seen_inventory_status = 1
				protocol_step = 3
			} else if (name == "pool") {
				if (seen_pool_status != 0 || protocol_step != 3) {
					fail()
				}
				pool_status = value
				seen_pool_status = 1
				protocol_step = 4
			} else if (name == "snapshot") {
				if (seen_snapshot_status != 0 || protocol_step != 8) {
					fail()
				}
				snapshot_status = value
				seen_snapshot_status = 1
				protocol_step = 9
			} else if (name == "snapshot_ran") {
				if (seen_snapshot_ran_status != 0 || protocol_step != 4) {
					fail()
				}
				snapshot_ran_status = value
				seen_snapshot_ran_status = 1
				protocol_step = 5
			} else {
				fail()
			}
		}'
}

# Purpose: Render parser section routing and staged-output initialization.
# Usage: Second AWK support stage; section order is validated before any body
# line is routed to a workspace file.
zxfer_render_remote_destination_discovery_batch_parser_section_stage() {
	# shellcheck disable=SC2016  # awk program should see literal $0.
	g_zxfer_remote_snapshot_discovery_batch_parser_program=$g_zxfer_remote_snapshot_discovery_batch_parser_program'
		function begin_section(name) {
			if (current_section != "") {
				fail()
			}
			if (name == "inventory_stdout") {
				if (seen_inventory_stdout != 0 || protocol_step != 5) {
					fail()
				}
				current_output = dest_out
				seen_inventory_stdout = 1
			} else if (name == "inventory_stderr") {
				if (seen_inventory_stderr != 0 || protocol_step != 6) {
					fail()
				}
				current_output = dest_err
				seen_inventory_stderr = 1
			} else if (name == "pool_stderr") {
				if (seen_pool_stderr != 0 || protocol_step != 7) {
					fail()
				}
				current_output = ""
				seen_pool_stderr = 1
			} else if (name == "snapshot_stdout") {
				if (seen_snapshot_stdout != 0 || protocol_step != 1) {
					fail()
				}
				current_output = snap_out
				seen_snapshot_stdout = 1
			} else if (name == "snapshot_stderr") {
				if (seen_snapshot_stderr != 0 || protocol_step != 9) {
					fail()
				}
				current_output = snap_err
				seen_snapshot_stderr = 1
			} else {
				fail()
			}
			current_section = name
		}
		function append_section_line(line) {
			if (current_output != "") {
				print line >> current_output
			}
		}
		BEGIN {
			tab = sprintf("%c", 9)
			current_section = ""
			current_output = ""
			protocol_step = 0
		}'
}

# Purpose: Render the parser record-state machine.
# Usage: Second fixed AWK stage; validates sentinels, sections, and statuses.
zxfer_render_remote_destination_discovery_batch_parser_records_stage() {
	# shellcheck disable=SC2016  # awk program should see literal $0.
	g_zxfer_remote_snapshot_discovery_batch_parser_program=$g_zxfer_remote_snapshot_discovery_batch_parser_program'
		{
			if (bad != 0) {
				next
			}
				if (seen_header == 0) {
					if ($0 != "ZXFER_DESTINATION_DISCOVERY_BATCH_V1") {
						fail()
					}
					seen_header = 1
					protocol_step = 1
					next
				}
				if ($0 == "ZXFER_DESTINATION_DISCOVERY_BATCH_END") {
					if (seen_end != 0 || current_section != "" || protocol_step != 10) {
						fail()
					}
					seen_end = 1
					protocol_step = 11
					next
			}
			if (seen_end != 0) {
				if ($0 != "") {
					fail()
				}
				next
			}
			if (current_section != "") {
				if (index($0, "END" tab) == 1) {
					section_name = substr($0, 5)
						if (section_name == current_section) {
							current_section = ""
							current_output = ""
							protocol_step++
							next
					}
				}
				append_section_line($0)
				next
			}
			if (index($0, "STATUS" tab) == 1) {
				status_record = substr($0, 8)
				status_separator = index(status_record, tab)
				if (status_separator == 0) {
					fail()
				}
				record_status(substr(status_record, 1, status_separator - 1), substr(status_record, status_separator + 1))
				next
			}
			if (index($0, "BEGIN" tab) == 1) {
				begin_section(substr($0, 7))
				next
			}
			if (index($0, "END" tab) == 1) {
				fail()
				next
			}
			fail()
		}'
}

# Purpose: Render parser completeness checks and staged-file finalization.
# Usage: Final fixed AWK stage; writes the compact status sidecar only after
# every required status and section has been observed exactly once.
zxfer_render_remote_destination_discovery_batch_parser_finalize_stage() {
	g_zxfer_remote_snapshot_discovery_batch_parser_program=$g_zxfer_remote_snapshot_discovery_batch_parser_program'
		END {
			if (bad != 0) {
				exit 1
			}
			if (seen_header != 1 || seen_end != 1 || current_section != "" || protocol_step != 11) {
				exit 1
			}
			if (seen_inventory_status != 1 || seen_pool_status != 1 || seen_snapshot_status != 1 || seen_snapshot_ran_status != 1) {
				exit 1
			}
			if (seen_inventory_stdout != 1 || seen_inventory_stderr != 1 || seen_pool_stderr != 1 || seen_snapshot_stdout != 1 || seen_snapshot_stderr != 1) {
				exit 1
			}
			print "inventory" tab inventory_status > status_out
			print "pool" tab pool_status > status_out
			print "snapshot" tab snapshot_status > status_out
			print "snapshot_ran" tab snapshot_ran_status > status_out
			close(status_out)
			close(dest_out)
			close(dest_err)
			close(snap_out)
			close(snap_err)
		}
	'
}

# Purpose: Build the streamed batch parser program in the current shell.
# Usage: Called once per payload before the single existing AWK invocation.
zxfer_build_remote_destination_discovery_batch_parser_program() {
	zxfer_reset_remote_destination_discovery_batch_parser_state
	zxfer_render_remote_destination_discovery_batch_parser_status_stage
	zxfer_render_remote_destination_discovery_batch_parser_section_stage
	zxfer_render_remote_destination_discovery_batch_parser_records_stage
	zxfer_render_remote_destination_discovery_batch_parser_finalize_stage
}

# Purpose: Split target-side destination discovery output into staged files and
# a compact status sidecar.
# Usage: Called with batch payload on stdin so large snapshot sections can be
# streamed through awk into final staging files instead of captured wholesale.
zxfer_split_remote_destination_discovery_batch_stream_to_files() {
	l_remote_batch_split_status_file=$1
	l_remote_batch_split_inventory_file=$2
	l_remote_batch_split_inventory_stderr_file=$3
	l_remote_batch_split_snapshot_file=$4
	l_remote_batch_split_snapshot_stderr_file=$5

	zxfer_build_remote_destination_discovery_batch_parser_program
	"${g_cmd_awk:-awk}" \
		-v dest_out="$l_remote_batch_split_inventory_file" \
		-v dest_err="$l_remote_batch_split_inventory_stderr_file" \
		-v snap_out="$l_remote_batch_split_snapshot_file" \
		-v snap_err="$l_remote_batch_split_snapshot_stderr_file" \
		-v status_out="$l_remote_batch_split_status_file" \
		"$g_zxfer_remote_snapshot_discovery_batch_parser_program"
}

# Purpose: Prepare and prevalidate the one target-side discovery command.
# Usage: Runs before local workspace allocation so script, quoting, transport,
# and wrapper-host failures leave no operation artifacts behind.
zxfer_prepare_remote_destination_discovery_batch_command() {
	l_remote_batch_prepare_dataset=$1
	l_remote_batch_prepare_pool=${g_destination%%/*}
	g_zxfer_remote_destination_discovery_command_result=""

	if l_remote_batch_prepare_script=$(zxfer_build_remote_destination_discovery_batch_script \
		"$g_destination" "$l_remote_batch_prepare_dataset" \
		"$l_remote_batch_prepare_pool"); then
		:
	else
		l_remote_batch_prepare_status=$?
		return "$l_remote_batch_prepare_status"
	fi
	if l_remote_batch_prepare_command=$(zxfer_build_remote_sh_c_command \
		"$l_remote_batch_prepare_script"); then
		:
	else
		l_remote_batch_prepare_status=$?
		return "$l_remote_batch_prepare_status"
	fi
	if l_remote_batch_prepare_transport=$(zxfer_get_ssh_transport_tokens_for_host \
		"$g_option_T_target_host"); then
		:
	else
		l_remote_batch_prepare_status=$?
		zxfer_throw_error "$l_remote_batch_prepare_transport" \
			"$l_remote_batch_prepare_status"
		return "$l_remote_batch_prepare_status"
	fi
	# Prevalidate wrapper-style host specs outside the streaming pipeline so
	# setup failures still exit through the parent shell's reporting path.
	if zxfer_prepare_ssh_shell_command_context \
		"$g_option_T_target_host" "$l_remote_batch_prepare_command"; then
		:
	else
		l_remote_batch_prepare_status=$?
		if [ -n "${g_zxfer_ssh_shell_context_error_result:-}" ]; then
			zxfer_throw_error "$g_zxfer_ssh_shell_context_error_result"
		fi
		return "$l_remote_batch_prepare_status"
	fi

	g_zxfer_remote_destination_discovery_command_result=$l_remote_batch_prepare_command
}

# Purpose: Validate one caller-visible discovery file before transactional use.
# Usage: Output paths must be regular direct children of the current private
# run root; workspace publication never accepts an unchecked external path.
zxfer_remote_destination_discovery_publish_target_is_valid() {
	l_remote_batch_target_path=$1
	zxfer_runtime_artifact_path_is_run_root_child \
		"$l_remote_batch_target_path" || return 1
	l_remote_batch_target_name=${l_remote_batch_target_path##*/}
	case "$l_remote_batch_target_name" in
	'' | *'
'* | rollback | transport.status | transport.stderr | batch.status)
		return 1
		;;
	esac
	[ -f "$l_remote_batch_target_path" ] || return 1
	[ ! -L "$l_remote_batch_target_path" ] &&
		[ ! -h "$l_remote_batch_target_path" ]
}

# Purpose: Initialize the fixed files inside one contained local workspace.
# Usage: Called before SSH starts so unwritable staging fails without opening a
# transport; descendants are removed only through their owning workspace.
zxfer_initialize_remote_destination_discovery_workspace_files() {
	for l_remote_batch_workspace_init_file in \
		"$g_zxfer_remote_destination_discovery_transport_status_file" \
		"$g_zxfer_remote_destination_discovery_transport_stderr_file" \
		"$g_zxfer_remote_destination_discovery_batch_status_file" \
		"$g_zxfer_remote_destination_discovery_inventory_stage_file" \
		"$g_zxfer_remote_destination_discovery_inventory_stderr_stage_file" \
		"$g_zxfer_remote_destination_discovery_snapshot_stage_file" \
		"$g_zxfer_remote_destination_discovery_snapshot_stderr_stage_file"; do
		zxfer_write_runtime_artifact_file \
			"$l_remote_batch_workspace_init_file" "" || return "$?"
	done
}

# Purpose: Allocate one private run-root child for the complete local protocol.
# Usage: The four staged payload basenames match their caller targets so normal
# backup and publish each require one multi-file rename operation.
zxfer_allocate_remote_destination_discovery_workspace() {
	g_zxfer_remote_destination_discovery_inventory_target_file=$1
	g_zxfer_remote_destination_discovery_inventory_stderr_target_file=$2
	g_zxfer_remote_destination_discovery_snapshot_target_file=$3
	g_zxfer_remote_destination_discovery_snapshot_stderr_target_file=$4

	for l_remote_batch_workspace_target in \
		"$g_zxfer_remote_destination_discovery_inventory_target_file" \
		"$g_zxfer_remote_destination_discovery_inventory_stderr_target_file" \
		"$g_zxfer_remote_destination_discovery_snapshot_target_file" \
		"$g_zxfer_remote_destination_discovery_snapshot_stderr_target_file"; do
		zxfer_remote_destination_discovery_publish_target_is_valid \
			"$l_remote_batch_workspace_target" || return 1
	done

	l_remote_batch_workspace_inventory_name=${g_zxfer_remote_destination_discovery_inventory_target_file##*/}
	l_remote_batch_workspace_inventory_stderr_name=${g_zxfer_remote_destination_discovery_inventory_stderr_target_file##*/}
	l_remote_batch_workspace_snapshot_name=${g_zxfer_remote_destination_discovery_snapshot_target_file##*/}
	l_remote_batch_workspace_snapshot_stderr_name=${g_zxfer_remote_destination_discovery_snapshot_stderr_target_file##*/}
	if [ "$l_remote_batch_workspace_inventory_name" = "$l_remote_batch_workspace_inventory_stderr_name" ] ||
		[ "$l_remote_batch_workspace_inventory_name" = "$l_remote_batch_workspace_snapshot_name" ] ||
		[ "$l_remote_batch_workspace_inventory_name" = "$l_remote_batch_workspace_snapshot_stderr_name" ] ||
		[ "$l_remote_batch_workspace_inventory_stderr_name" = "$l_remote_batch_workspace_snapshot_name" ] ||
		[ "$l_remote_batch_workspace_inventory_stderr_name" = "$l_remote_batch_workspace_snapshot_stderr_name" ] ||
		[ "$l_remote_batch_workspace_snapshot_name" = "$l_remote_batch_workspace_snapshot_stderr_name" ]; then
		return 1
	fi

	zxfer_create_private_temp_dir \
		"zxfer-remote-destination-discovery" >/dev/null || return "$?"
	g_zxfer_remote_destination_discovery_workspace=$g_zxfer_runtime_artifact_path_result
	g_zxfer_remote_destination_discovery_rollback_dir="$g_zxfer_remote_destination_discovery_workspace/rollback"
	mkdir -m 700 "$g_zxfer_remote_destination_discovery_rollback_dir" \
		2>/dev/null || return "$?"
	g_zxfer_remote_destination_discovery_transport_status_file="$g_zxfer_remote_destination_discovery_workspace/transport.status"
	g_zxfer_remote_destination_discovery_transport_stderr_file="$g_zxfer_remote_destination_discovery_workspace/transport.stderr"
	g_zxfer_remote_destination_discovery_batch_status_file="$g_zxfer_remote_destination_discovery_workspace/batch.status"
	g_zxfer_remote_destination_discovery_inventory_stage_file="$g_zxfer_remote_destination_discovery_workspace/$l_remote_batch_workspace_inventory_name"
	g_zxfer_remote_destination_discovery_inventory_stderr_stage_file="$g_zxfer_remote_destination_discovery_workspace/$l_remote_batch_workspace_inventory_stderr_name"
	g_zxfer_remote_destination_discovery_snapshot_stage_file="$g_zxfer_remote_destination_discovery_workspace/$l_remote_batch_workspace_snapshot_name"
	g_zxfer_remote_destination_discovery_snapshot_stderr_stage_file="$g_zxfer_remote_destination_discovery_workspace/$l_remote_batch_workspace_snapshot_stderr_name"

	zxfer_initialize_remote_destination_discovery_workspace_files
}

# Purpose: Execute the one SSH-to-parser pipeline for the active workspace.
# Usage: Transport status crosses the POSIX pipeline boundary through a compact
# file because supported shells do not provide pipefail or shared pipe state.
zxfer_execute_remote_destination_discovery_batch_pipeline() {
	g_zxfer_remote_destination_discovery_parser_status_result=0
	zxfer_echoV "Running remote destination discovery batch for $g_destination."
	{
		l_remote_batch_pipeline_transport_status=0
		zxfer_invoke_ssh_shell_command_for_host \
			"$g_option_T_target_host" \
			"$g_zxfer_remote_destination_discovery_command_result" \
			destination \
			2>"$g_zxfer_remote_destination_discovery_transport_stderr_file" ||
			l_remote_batch_pipeline_transport_status=$?
		printf '%s\n' "$l_remote_batch_pipeline_transport_status" \
			>"$g_zxfer_remote_destination_discovery_transport_status_file" || :
	} | zxfer_split_remote_destination_discovery_batch_stream_to_files \
		"$g_zxfer_remote_destination_discovery_batch_status_file" \
		"$g_zxfer_remote_destination_discovery_inventory_stage_file" \
		"$g_zxfer_remote_destination_discovery_inventory_stderr_stage_file" \
		"$g_zxfer_remote_destination_discovery_snapshot_stage_file" \
		"$g_zxfer_remote_destination_discovery_snapshot_stderr_stage_file" ||
		g_zxfer_remote_destination_discovery_parser_status_result=$?
}

# Purpose: Load and validate the transport status written by the pipeline side.
# Usage: Publishes a failure kind so the post-cleanup reporter can preserve the
# old malformed-status message without throwing while the workspace is live.
zxfer_load_remote_destination_discovery_transport_status() {
	if zxfer_read_snapshot_discovery_capture_file \
		"$g_zxfer_remote_destination_discovery_transport_status_file"; then
		:
	else
		l_remote_batch_transport_status_read_failure=$?
		g_zxfer_remote_destination_discovery_failure_kind=transport_status_read
		g_zxfer_remote_destination_discovery_failure_status=$l_remote_batch_transport_status_read_failure
		return "$g_zxfer_remote_destination_discovery_failure_status"
	fi
	l_remote_batch_transport_status=$g_zxfer_snapshot_discovery_file_read_result
	case "$l_remote_batch_transport_status" in
	*'
')
		l_remote_batch_transport_status=${l_remote_batch_transport_status%?}
		;;
	esac
	case "$l_remote_batch_transport_status" in
	'' | *[!0-9]*)
		g_zxfer_remote_destination_discovery_failure_kind=transport_status_malformed
		g_zxfer_remote_destination_discovery_failure_status=1
		return 1
		;;
	esac
	g_zxfer_remote_destination_discovery_transport_status_result=$l_remote_batch_transport_status
}

# Purpose: Capture transport stderr after a validated nonzero SSH status.
# Usage: The full collector retrieves this through an accessor so malformed
# protocol output is never published merely to preserve the SSH diagnostic.
zxfer_load_remote_destination_discovery_transport_stderr() {
	if zxfer_read_snapshot_discovery_capture_file \
		"$g_zxfer_remote_destination_discovery_transport_stderr_file"; then
		g_zxfer_remote_destination_discovery_transport_stderr_result=$g_zxfer_snapshot_discovery_file_read_result
		return 0
	else
		l_remote_batch_transport_stderr_read_failure=$?
		g_zxfer_remote_destination_discovery_failure_kind=transport_stderr_read
		g_zxfer_remote_destination_discovery_failure_status=$l_remote_batch_transport_stderr_read_failure
		return "$g_zxfer_remote_destination_discovery_failure_status"
	fi
}

# Purpose: Validate the complete staged payload set before publication.
# Usage: Parser success alone is insufficient if a file was replaced, removed,
# or made unreadable between AWK finalization and the publish transaction.
zxfer_validate_remote_destination_discovery_workspace_files() {
	for l_remote_batch_readback_file in \
		"$g_zxfer_remote_destination_discovery_batch_status_file" \
		"$g_zxfer_remote_destination_discovery_inventory_stage_file" \
		"$g_zxfer_remote_destination_discovery_inventory_stderr_stage_file" \
		"$g_zxfer_remote_destination_discovery_snapshot_stage_file" \
		"$g_zxfer_remote_destination_discovery_snapshot_stderr_stage_file"; do
		[ -f "$l_remote_batch_readback_file" ] &&
			[ -r "$l_remote_batch_readback_file" ] &&
			[ ! -L "$l_remote_batch_readback_file" ] &&
			[ ! -h "$l_remote_batch_readback_file" ] || return 1
	done
}

# Purpose: Move all four prior caller files into the workspace rollback slot.
# Usage: One multi-file move keeps the normal helper-spawn budget below the old
# three-file allocation and cleanup path.
zxfer_backup_remote_destination_discovery_publish_targets() {
	mv -f \
		"$g_zxfer_remote_destination_discovery_inventory_target_file" \
		"$g_zxfer_remote_destination_discovery_inventory_stderr_target_file" \
		"$g_zxfer_remote_destination_discovery_snapshot_target_file" \
		"$g_zxfer_remote_destination_discovery_snapshot_stderr_target_file" \
		"$g_zxfer_remote_destination_discovery_rollback_dir" 2>/dev/null
}

# Purpose: Publish all four validated staged payloads into the run root.
# Usage: Kept behind one abstraction so failure tests can simulate a later-file
# rename failure after making one new file visible.
zxfer_publish_remote_destination_discovery_staged_files() {
	mv -f \
		"$g_zxfer_remote_destination_discovery_inventory_stage_file" \
		"$g_zxfer_remote_destination_discovery_inventory_stderr_stage_file" \
		"$g_zxfer_remote_destination_discovery_snapshot_stage_file" \
		"$g_zxfer_remote_destination_discovery_snapshot_stderr_stage_file" \
		"$g_zxfer_run_tmp_root" 2>/dev/null
}

# Purpose: Restore every available rollback file to its caller-visible path.
# Usage: The fast path restores all four in one move; the partial-backup path
# checks fixed pairs individually because a failed multi-file move may stop late.
zxfer_restore_remote_destination_discovery_publish_targets() {
	l_remote_batch_restore_inventory="$g_zxfer_remote_destination_discovery_rollback_dir/${g_zxfer_remote_destination_discovery_inventory_target_file##*/}"
	l_remote_batch_restore_inventory_stderr="$g_zxfer_remote_destination_discovery_rollback_dir/${g_zxfer_remote_destination_discovery_inventory_stderr_target_file##*/}"
	l_remote_batch_restore_snapshot="$g_zxfer_remote_destination_discovery_rollback_dir/${g_zxfer_remote_destination_discovery_snapshot_target_file##*/}"
	l_remote_batch_restore_snapshot_stderr="$g_zxfer_remote_destination_discovery_rollback_dir/${g_zxfer_remote_destination_discovery_snapshot_stderr_target_file##*/}"

	if [ -f "$l_remote_batch_restore_inventory" ] &&
		[ -f "$l_remote_batch_restore_inventory_stderr" ] &&
		[ -f "$l_remote_batch_restore_snapshot" ] &&
		[ -f "$l_remote_batch_restore_snapshot_stderr" ]; then
		mv -f "$l_remote_batch_restore_inventory" \
			"$l_remote_batch_restore_inventory_stderr" \
			"$l_remote_batch_restore_snapshot" \
			"$l_remote_batch_restore_snapshot_stderr" \
			"$g_zxfer_run_tmp_root" 2>/dev/null
		return
	fi

	l_remote_batch_restore_status=0
	[ ! -f "$l_remote_batch_restore_inventory" ] ||
		mv -f "$l_remote_batch_restore_inventory" \
			"$g_zxfer_remote_destination_discovery_inventory_target_file" 2>/dev/null ||
		l_remote_batch_restore_status=1
	[ ! -f "$l_remote_batch_restore_inventory_stderr" ] ||
		mv -f "$l_remote_batch_restore_inventory_stderr" \
			"$g_zxfer_remote_destination_discovery_inventory_stderr_target_file" 2>/dev/null ||
		l_remote_batch_restore_status=1
	[ ! -f "$l_remote_batch_restore_snapshot" ] ||
		mv -f "$l_remote_batch_restore_snapshot" \
			"$g_zxfer_remote_destination_discovery_snapshot_target_file" 2>/dev/null ||
		l_remote_batch_restore_status=1
	[ ! -f "$l_remote_batch_restore_snapshot_stderr" ] ||
		mv -f "$l_remote_batch_restore_snapshot_stderr" \
			"$g_zxfer_remote_destination_discovery_snapshot_stderr_target_file" 2>/dev/null ||
		l_remote_batch_restore_status=1
	return "$l_remote_batch_restore_status"
}

# Purpose: Clear all four caller files when rollback itself cannot complete.
# Usage: Mixed old/new discovery state must never survive a failed transaction;
# cleared transient files make the operation fail closed instead.
zxfer_clear_remote_destination_discovery_publish_targets() {
	l_remote_batch_clear_status=0
	for l_remote_batch_clear_target in \
		"$g_zxfer_remote_destination_discovery_inventory_target_file" \
		"$g_zxfer_remote_destination_discovery_inventory_stderr_target_file" \
		"$g_zxfer_remote_destination_discovery_snapshot_target_file" \
		"$g_zxfer_remote_destination_discovery_snapshot_stderr_target_file"; do
		zxfer_write_runtime_artifact_file "$l_remote_batch_clear_target" "" ||
			l_remote_batch_clear_status=1
	done
	return "$l_remote_batch_clear_status"
}

# Purpose: Atomically publish the validated four-file discovery result set.
# Usage: Any backup or later-file publish failure restores all prior files; a
# failed restore clears all four targets rather than exposing mixed generations.
zxfer_publish_remote_destination_discovery_workspace_files() {
	if zxfer_backup_remote_destination_discovery_publish_targets; then
		:
	else
		l_remote_batch_publish_status=$?
		zxfer_restore_remote_destination_discovery_publish_targets ||
			zxfer_clear_remote_destination_discovery_publish_targets || :
		return "$l_remote_batch_publish_status"
	fi
	if zxfer_publish_remote_destination_discovery_staged_files; then
		return 0
	else
		l_remote_batch_publish_status=$?
		zxfer_restore_remote_destination_discovery_publish_targets ||
			zxfer_clear_remote_destination_discovery_publish_targets || :
		return "$l_remote_batch_publish_status"
	fi
}

# Purpose: Record the target-side ZFS calls represented by one valid batch.
# Usage: Called only after publication so malformed or transport-failed payloads
# cannot inflate successful destination discovery counters.
zxfer_profile_remote_destination_discovery_batch_calls() {
	zxfer_profile_record_zfs_call destination list
	[ -z "${g_zxfer_destination_discovery_batch_pool_status:-}" ] ||
		zxfer_profile_record_zfs_call destination list
	[ "${g_zxfer_destination_discovery_batch_snapshot_ran:-0}" -ne 1 ] ||
		zxfer_profile_record_zfs_call destination list
}

# Purpose: Execute, validate, and publish one prepared workspace protocol.
# Usage: Does not clean or throw; the outer coordinator performs exactly one
# normal workspace cleanup before translating the recorded failure kind.
zxfer_process_remote_destination_discovery_workspace() {
	zxfer_execute_remote_destination_discovery_batch_pipeline
	zxfer_load_remote_destination_discovery_transport_status || return "$?"
	if [ "$g_zxfer_remote_destination_discovery_transport_status_result" -ne 0 ]; then
		zxfer_load_remote_destination_discovery_transport_stderr || return "$?"
		g_zxfer_remote_destination_discovery_failure_kind=transport
		g_zxfer_remote_destination_discovery_failure_status=$g_zxfer_remote_destination_discovery_transport_status_result
		return "$g_zxfer_remote_destination_discovery_failure_status"
	fi
	if [ "$g_zxfer_remote_destination_discovery_parser_status_result" -ne 0 ]; then
		g_zxfer_remote_destination_discovery_failure_kind=batch_parse
		g_zxfer_remote_destination_discovery_failure_status=$g_zxfer_remote_destination_discovery_parser_status_result
		return "$g_zxfer_remote_destination_discovery_failure_status"
	fi
	if zxfer_load_destination_discovery_batch_status_file \
		"$g_zxfer_remote_destination_discovery_batch_status_file"; then
		:
	else
		l_remote_batch_status_load_failure=$?
		g_zxfer_remote_destination_discovery_failure_kind=batch_status
		g_zxfer_remote_destination_discovery_failure_status=$l_remote_batch_status_load_failure
		return "$g_zxfer_remote_destination_discovery_failure_status"
	fi
	if zxfer_validate_remote_destination_discovery_workspace_files; then
		:
	else
		l_remote_batch_readback_failure=$?
		g_zxfer_remote_destination_discovery_failure_kind=batch_readback
		g_zxfer_remote_destination_discovery_failure_status=$l_remote_batch_readback_failure
		return "$g_zxfer_remote_destination_discovery_failure_status"
	fi
	if zxfer_publish_remote_destination_discovery_workspace_files; then
		:
	else
		l_remote_batch_publish_failure=$?
		g_zxfer_remote_destination_discovery_failure_kind=batch_publish
		g_zxfer_remote_destination_discovery_failure_status=$l_remote_batch_publish_failure
		return "$g_zxfer_remote_destination_discovery_failure_status"
	fi
	zxfer_profile_remote_destination_discovery_batch_calls
}

# Purpose: Clean the one contained local discovery workspace.
# Usage: Called once after every post-allocation processing result; whole-run
# trap cleanup remains the fallback if this checked direct-child cleanup fails.
zxfer_cleanup_remote_destination_discovery_workspace() {
	l_remote_batch_cleanup_workspace=${g_zxfer_remote_destination_discovery_workspace:-}
	if [ -n "$l_remote_batch_cleanup_workspace" ]; then
		zxfer_cleanup_runtime_artifact_path \
			"$l_remote_batch_cleanup_workspace" >/dev/null 2>&1 || :
	fi
	zxfer_reset_remote_destination_discovery_workspace_state
}

# Purpose: Translate one post-cleanup protocol failure to its legacy contract.
# Usage: Keeps reporting out of staging helpers so throw/exit cannot bypass the
# operation's normal workspace cleanup.
zxfer_report_remote_destination_discovery_failure() {
	case "$g_zxfer_remote_destination_discovery_failure_kind" in
	transport | transport_status_read | transport_stderr_read)
		return "$g_zxfer_remote_destination_discovery_failure_status"
		;;
	transport_status_malformed)
		zxfer_throw_error "Malformed destination discovery transport status."
		return 1
		;;
	batch_parse | batch_status | batch_readback | batch_publish)
		zxfer_throw_error "Malformed destination discovery batch response." \
			"$g_zxfer_remote_destination_discovery_failure_status"
		return "$g_zxfer_remote_destination_discovery_failure_status"
		;;
	esac
	return "${g_zxfer_remote_destination_discovery_failure_status:-1}"
}

# Purpose: Return the last validated transport stderr diagnostic.
# Usage: Full discovery uses this only after a numeric nonzero SSH status; raw
# malformed protocol output never becomes caller-visible staged data.
zxfer_get_remote_destination_discovery_transport_stderr() {
	printf '%s' "${g_zxfer_remote_destination_discovery_transport_stderr_result:-}"
}

# Purpose: Return whether the last batch failure was a validated SSH failure.
# Usage: Orchestration uses this owner operation before staging the captured
# transport diagnostic separately from the four transaction output files.
zxfer_remote_destination_discovery_failure_is_transport() {
	[ "${g_zxfer_remote_destination_discovery_failure_kind:-}" = transport ]
}

# Purpose: Run target-side destination discovery through one remote SSH shell
# invocation and publish its complete result set transactionally.
# Usage: Called by snapshot discovery when `-T` is active; caller files remain
# untouched until transport, sentinels, sections, statuses, and readback pass.
zxfer_run_remote_destination_discovery_batch_to_files() {
	l_remote_batch_run_dataset=$1
	l_remote_batch_run_inventory_target=$2
	l_remote_batch_run_inventory_stderr_target=$3
	l_remote_batch_run_snapshot_target=$4
	l_remote_batch_run_snapshot_stderr_target=$5

	zxfer_reset_destination_discovery_batch_state
	zxfer_prepare_remote_destination_discovery_batch_command \
		"$l_remote_batch_run_dataset" || return "$?"
	if zxfer_allocate_remote_destination_discovery_workspace \
		"$l_remote_batch_run_inventory_target" \
		"$l_remote_batch_run_inventory_stderr_target" \
		"$l_remote_batch_run_snapshot_target" \
		"$l_remote_batch_run_snapshot_stderr_target"; then
		:
	else
		l_remote_batch_run_status=$?
		zxfer_cleanup_remote_destination_discovery_workspace
		return "$l_remote_batch_run_status"
	fi

	zxfer_process_remote_destination_discovery_workspace
	l_remote_batch_run_status=$?
	zxfer_cleanup_remote_destination_discovery_workspace
	[ "$l_remote_batch_run_status" -eq 0 ] ||
		zxfer_report_remote_destination_discovery_failure
}
