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
# shellcheck shell=sh

zxfer_cleanup_child_wrapper_list_descendants() {
	l_cleanup_wrapper_snapshot_status=0
	l_cleanup_wrapper_snapshot=$(ps -o pid= -o ppid= 2>/dev/null) || l_cleanup_wrapper_snapshot_status=$?
	[ "$l_cleanup_wrapper_snapshot_status" -eq 0 ] || return "$l_cleanup_wrapper_snapshot_status"

	printf '%s\n' "$l_cleanup_wrapper_snapshot" | awk -v root="$$" '
	{
		pid = $1
		ppid = $2
		if (pid != "") {
			parent[pid] = ppid
			seen[pid] = 1
		}
	}
	END {
		target[root] = 1
		changed = 1
		while (changed) {
			changed = 0
			for (pid in seen) {
				if ((parent[pid] in target) && !(pid in target)) {
					target[pid] = 1
					changed = 1
				}
			}
		}
		for (pid in target) {
			if (pid != root) {
				print pid
			}
		}
	}' | LC_ALL=C sort -nr
}

zxfer_cleanup_child_wrapper_abort_descendants() {
	l_cleanup_wrapper_descendants_status=0
	l_cleanup_wrapper_descendants=$(zxfer_cleanup_child_wrapper_list_descendants) ||
		l_cleanup_wrapper_descendants_status=$?
	[ "$l_cleanup_wrapper_descendants_status" -eq 0 ] || return "$l_cleanup_wrapper_descendants_status"

	while IFS= read -r l_cleanup_wrapper_pid || [ -n "$l_cleanup_wrapper_pid" ]; do
		[ -n "$l_cleanup_wrapper_pid" ] || continue
		kill -s TERM "$l_cleanup_wrapper_pid" 2>/dev/null || :
	done <<-EOF
		$l_cleanup_wrapper_descendants
	EOF
}

zxfer_cleanup_child_wrapper_on_signal() {
	zxfer_cleanup_child_wrapper_abort_descendants >/dev/null 2>&1 || :
	exit 143
}

zxfer_cleanup_child_wrapper_main() {
	l_cleanup_wrapper_exec_cmd=$1

	[ -n "$l_cleanup_wrapper_exec_cmd" ] || return 1
	trap 'zxfer_cleanup_child_wrapper_on_signal' TERM INT HUP
	l_cleanup_wrapper_status=0
	exec 3<&0 || l_cleanup_wrapper_status=$?
	[ "$l_cleanup_wrapper_status" -eq 0 ] || return "$l_cleanup_wrapper_status"

	# Preserve the wrapper's stdin for background children. Some /bin/sh
	# implementations reattach asynchronous jobs to /dev/null unless stdin is
	# duplicated onto a dedicated descriptor before the background launch.
	sh -c "$l_cleanup_wrapper_exec_cmd" <&3 &
	l_cleanup_wrapper_child_pid=$!
	l_cleanup_wrapper_status=0
	wait "$l_cleanup_wrapper_child_pid" || l_cleanup_wrapper_status=$?
	exec 3<&-
	return "$l_cleanup_wrapper_status"
}

if [ "${ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY:-0}" != "1" ]; then
	zxfer_cleanup_child_wrapper_main "$@"
	exit $?
fi
