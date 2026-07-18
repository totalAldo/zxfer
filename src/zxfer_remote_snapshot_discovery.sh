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
#   snapshot, and snapshot-ran state; in-shell script and parser renderer
#   result channels.
# reads globals: target-host execution context, resolved remote helpers,
#   destination roots, compression settings, and producer staging helpers.
# mutates caches: none.
# returns via stdout: rendered remote batch scripts and parsed batch status;
#   writes separated inventory, stderr, and snapshot streams to caller paths.

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
	g_zxfer_destination_discovery_batch_inventory_status=""
	g_zxfer_destination_discovery_batch_pool_status=""
	g_zxfer_destination_discovery_batch_snapshot_status=""
	g_zxfer_destination_discovery_batch_snapshot_ran=""
	zxfer_reset_remote_destination_discovery_batch_script_state
	zxfer_reset_remote_destination_discovery_batch_parser_state
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
	l_remote_destination_discovery_batch_script_dependency_path=$(zxfer_get_effective_dependency_path)
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
	zxfer_prepare_remote_destination_discovery_batch_script_inputs "$1" "$2" "$3"
	zxfer_render_remote_destination_discovery_batch_script_support_stage
	zxfer_render_remote_destination_discovery_batch_script_staging_stage
	zxfer_render_remote_destination_discovery_batch_script_discovery_stage
	zxfer_render_remote_destination_discovery_batch_script_classification_stage
	zxfer_render_remote_destination_discovery_batch_script_publication_stage

	cat <<-EOF
		$g_zxfer_remote_snapshot_discovery_batch_script_result
	EOF
}

# Purpose: Load the compact status sidecar from destination discovery parsing.
# Usage: Called after the batch output file has been split into staged payload
# files without replaying large snapshot lists through a shell loop.
zxfer_load_destination_discovery_batch_status_file() {
	l_status_file=$1
	l_tab='	'

	zxfer_reset_destination_discovery_batch_state

	l_seen_inventory_status=0
	l_seen_pool_status=0
	l_seen_snapshot_status=0
	l_seen_snapshot_ran_status=0

	while IFS= read -r l_status_line || [ -n "$l_status_line" ]; do
		case "$l_status_line" in
		*"$l_tab"*)
			l_status_name=${l_status_line%%"$l_tab"*}
			l_status_value=${l_status_line#*"$l_tab"}
			;;
		*)
			return 1
			;;
		esac
		case "$l_status_name" in
		inventory)
			[ "$l_seen_inventory_status" -eq 0 ] || return 1
			g_zxfer_destination_discovery_batch_inventory_status=$l_status_value
			l_seen_inventory_status=1
			;;
		pool)
			[ "$l_seen_pool_status" -eq 0 ] || return 1
			g_zxfer_destination_discovery_batch_pool_status=$l_status_value
			l_seen_pool_status=1
			;;
		snapshot)
			[ "$l_seen_snapshot_status" -eq 0 ] || return 1
			g_zxfer_destination_discovery_batch_snapshot_status=$l_status_value
			l_seen_snapshot_status=1
			;;
		snapshot_ran)
			[ "$l_seen_snapshot_ran_status" -eq 0 ] || return 1
			g_zxfer_destination_discovery_batch_snapshot_ran=$l_status_value
			l_seen_snapshot_ran_status=1
			;;
		*)
			return 1
			;;
		esac
	done <"$l_status_file"

	[ "$l_seen_inventory_status" -eq 1 ] || return 1
	[ "$l_seen_pool_status" -eq 1 ] || return 1
	[ "$l_seen_snapshot_status" -eq 1 ] || return 1
	[ "$l_seen_snapshot_ran_status" -eq 1 ] || return 1
	zxfer_destination_discovery_batch_status_is_numeric "$g_zxfer_destination_discovery_batch_inventory_status" || return 1
	zxfer_destination_discovery_batch_status_is_numeric "$g_zxfer_destination_discovery_batch_snapshot_status" || return 1
	zxfer_destination_discovery_batch_status_is_numeric "$g_zxfer_destination_discovery_batch_snapshot_ran" || return 1
	if [ -n "$g_zxfer_destination_discovery_batch_pool_status" ]; then
		zxfer_destination_discovery_batch_status_is_numeric "$g_zxfer_destination_discovery_batch_pool_status" || return 1
	fi
}

# Purpose: Reset the in-shell AWK parser result channel.
# Usage: Called before parser stages append their fixed program fragments.
zxfer_reset_remote_destination_discovery_batch_parser_state() {
	g_zxfer_remote_snapshot_discovery_batch_parser_program=""
}

# Purpose: Render parser support functions and initialization.
# Usage: First fixed AWK stage for remote destination batch validation.
zxfer_render_remote_destination_discovery_batch_parser_support_stage() {
	# shellcheck disable=SC2016  # awk program should see literal $0.
	g_zxfer_remote_snapshot_discovery_batch_parser_program='
		function fail() {
			bad = 1
		}
		function record_status(name, value) {
			if (name == "inventory") {
				if (seen_inventory_status != 0) {
					fail()
				}
				inventory_status = value
				seen_inventory_status = 1
			} else if (name == "pool") {
				if (seen_pool_status != 0) {
					fail()
				}
				pool_status = value
				seen_pool_status = 1
			} else if (name == "snapshot") {
				if (seen_snapshot_status != 0) {
					fail()
				}
				snapshot_status = value
				seen_snapshot_status = 1
			} else if (name == "snapshot_ran") {
				if (seen_snapshot_ran_status != 0) {
					fail()
				}
				snapshot_ran_status = value
				seen_snapshot_ran_status = 1
			} else {
				fail()
			}
		}
		function begin_section(name) {
			if (current_section != "") {
				fail()
			}
			if (name == "inventory_stdout") {
				if (seen_inventory_stdout != 0) {
					fail()
				}
				current_output = dest_out
				seen_inventory_stdout = 1
			} else if (name == "inventory_stderr") {
				if (seen_inventory_stderr != 0) {
					fail()
				}
				current_output = dest_err
				seen_inventory_stderr = 1
			} else if (name == "pool_stderr") {
				if (seen_pool_stderr != 0) {
					fail()
				}
				current_output = ""
				seen_pool_stderr = 1
			} else if (name == "snapshot_stdout") {
				if (seen_snapshot_stdout != 0) {
					fail()
				}
				current_output = snap_out
				seen_snapshot_stdout = 1
			} else if (name == "snapshot_stderr") {
				if (seen_snapshot_stderr != 0) {
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
				next
			}
			if ($0 == "ZXFER_DESTINATION_DISCOVERY_BATCH_END") {
				if (current_section != "") {
					fail()
				}
				seen_end = 1
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
			if (seen_header != 1 || seen_end != 1 || current_section != "") {
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
	zxfer_render_remote_destination_discovery_batch_parser_support_stage
	zxfer_render_remote_destination_discovery_batch_parser_records_stage
	zxfer_render_remote_destination_discovery_batch_parser_finalize_stage
}

# Purpose: Split target-side destination discovery output into staged files and
# a compact status sidecar.
# Usage: Called with batch payload on stdin so large snapshot sections can be
# streamed through awk into final staging files instead of captured wholesale.
zxfer_split_remote_destination_discovery_batch_stream_to_files() {
	l_batch_status_file=$1
	l_dest_list_tmp_file=$2
	l_dest_list_err_file=$3
	l_rzfs_list_hr_snap_tmp_file=$4
	l_rzfs_list_hr_snap_err_tmp_file=$5

	zxfer_write_runtime_artifact_file "$l_dest_list_tmp_file" "" || return "$?"
	zxfer_write_runtime_artifact_file "$l_dest_list_err_file" "" || return "$?"
	zxfer_write_runtime_artifact_file "$l_rzfs_list_hr_snap_tmp_file" "" || return "$?"
	zxfer_write_runtime_artifact_file "$l_rzfs_list_hr_snap_err_tmp_file" "" || return "$?"
	zxfer_write_runtime_artifact_file "$l_batch_status_file" "" || return "$?"

	zxfer_build_remote_destination_discovery_batch_parser_program
	"${g_cmd_awk:-awk}" \
		-v dest_out="$l_dest_list_tmp_file" \
		-v dest_err="$l_dest_list_err_file" \
		-v snap_out="$l_rzfs_list_hr_snap_tmp_file" \
		-v snap_err="$l_rzfs_list_hr_snap_err_tmp_file" \
		-v status_out="$l_batch_status_file" \
		"$g_zxfer_remote_snapshot_discovery_batch_parser_program"
}

# Purpose: Run target-side destination discovery through one remote SSH shell
# invocation and stage its results.
# Usage: Called by snapshot discovery when `-T` is active to avoid separate
# target SSH round trips for destination dataset inventory and snapshot listing.
zxfer_run_remote_destination_discovery_batch_to_files() {
	l_destination_dataset=$1
	l_dest_list_tmp_file=$2
	l_dest_list_err_file=$3
	l_rzfs_list_hr_snap_tmp_file=$4
	l_rzfs_list_hr_snap_err_tmp_file=$5
	l_destination_pool=${g_destination%%/*}
	l_transport_status_file=""
	l_transport_stderr_file=""
	l_batch_status_file=""

	zxfer_reset_destination_discovery_batch_state

	l_remote_script=$(zxfer_build_remote_destination_discovery_batch_script \
		"$g_destination" "$l_destination_dataset" "$l_destination_pool") ||
		return "$?"
	l_remote_cmd=$(zxfer_build_remote_sh_c_command "$l_remote_script") ||
		return "$?"
	l_transport_tokens=$(zxfer_get_ssh_transport_tokens_for_host "$g_option_T_target_host") ||
		zxfer_throw_error "$l_transport_tokens" "$?"
	# Prevalidate wrapper-style host specs outside the streaming pipeline so
	# setup failures still exit through the parent shell's reporting path.
	if zxfer_prepare_ssh_shell_command_context "$g_option_T_target_host" "$l_remote_cmd"; then
		:
	else
		l_status=$?
		if [ "$g_zxfer_ssh_shell_context_error_result" != "" ]; then
			zxfer_throw_error "$g_zxfer_ssh_shell_context_error_result"
		fi
		return "$l_status"
	fi

	zxfer_get_temp_file >/dev/null || return "$?"
	l_transport_status_file=$g_zxfer_temp_file_result
	zxfer_get_temp_file >/dev/null || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_path "$l_transport_status_file"
		return "$l_status"
	}
	l_transport_stderr_file=$g_zxfer_temp_file_result
	zxfer_get_temp_file >/dev/null || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file"
		return "$l_status"
	}
	l_batch_status_file=$g_zxfer_temp_file_result

	zxfer_echoV "Running remote destination discovery batch for $g_destination."
	l_parse_status=0
	{
		l_transport_status=0
		zxfer_invoke_ssh_shell_command_for_host "$g_option_T_target_host" "$l_remote_cmd" destination 2>"$l_transport_stderr_file" ||
			l_transport_status=$?
		printf '%s\n' "$l_transport_status" >"$l_transport_status_file" || :
	} | zxfer_split_remote_destination_discovery_batch_stream_to_files \
		"$l_batch_status_file" \
		"$l_dest_list_tmp_file" \
		"$l_dest_list_err_file" \
		"$l_rzfs_list_hr_snap_tmp_file" \
		"$l_rzfs_list_hr_snap_err_tmp_file" || l_parse_status=$?

	zxfer_read_snapshot_discovery_capture_file "$l_transport_status_file" || {
		l_status=$?
		zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file" "$l_batch_status_file"
		return "$l_status"
	}
	l_batch_status=$g_zxfer_snapshot_discovery_file_read_result
	case "$l_batch_status" in
	*'
')
		l_batch_status=${l_batch_status%?}
		;;
	esac
	case "$l_batch_status" in
	'' | *[!0-9]*)
		zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file" "$l_batch_status_file"
		zxfer_throw_error "Malformed destination discovery transport status."
		;;
	esac
	if [ "$l_batch_status" -ne 0 ]; then
		zxfer_read_snapshot_discovery_capture_file "$l_transport_stderr_file" || {
			l_status=$?
			zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file" "$l_batch_status_file"
			return "$l_status"
		}
		l_transport_stderr=$g_zxfer_snapshot_discovery_file_read_result
		l_status=0
		zxfer_write_runtime_artifact_file "$l_dest_list_err_file" "$l_transport_stderr" || l_status=$?
		zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file" "$l_batch_status_file"
		if [ "$l_status" -ne 0 ]; then
			return "$l_status"
		fi
		return "$l_batch_status"
	fi

	if [ "$l_parse_status" -ne 0 ]; then
		zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file" "$l_batch_status_file"
		zxfer_throw_error "Malformed destination discovery batch response." "$l_parse_status"
	fi

	l_status=0
	zxfer_load_destination_discovery_batch_status_file "$l_batch_status_file" || l_status=$?
	zxfer_cleanup_runtime_artifact_paths "$l_transport_status_file" "$l_transport_stderr_file" "$l_batch_status_file"
	if [ "$l_status" -ne 0 ]; then
		zxfer_throw_error "Malformed destination discovery batch response." "$l_status"
	fi
	zxfer_profile_record_zfs_call destination list
	if [ -n "${g_zxfer_destination_discovery_batch_pool_status:-}" ]; then
		zxfer_profile_record_zfs_call destination list
	fi
	if [ "${g_zxfer_destination_discovery_batch_snapshot_ran:-0}" -eq 1 ]; then
		zxfer_profile_record_zfs_call destination list
	fi
}
