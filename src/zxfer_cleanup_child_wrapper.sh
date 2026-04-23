#!/bin/sh
# shellcheck shell=sh

zxfer_cleanup_child_wrapper_list_descendants() {
	l_cleanup_wrapper_snapshot=$(ps -o pid= -o ppid= 2>/dev/null) || return 1

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
	zxfer_cleanup_child_wrapper_list_descendants |
		while IFS= read -r l_cleanup_wrapper_pid || [ -n "$l_cleanup_wrapper_pid" ]; do
			[ -n "$l_cleanup_wrapper_pid" ] || continue
			kill -s TERM "$l_cleanup_wrapper_pid" 2>/dev/null || :
		done
}

zxfer_cleanup_child_wrapper_on_signal() {
	zxfer_cleanup_child_wrapper_abort_descendants >/dev/null 2>&1 || :
	exit 143
}

zxfer_cleanup_child_wrapper_main() {
	l_cleanup_wrapper_exec_cmd=$1

	[ -n "$l_cleanup_wrapper_exec_cmd" ] || return 1
	trap 'zxfer_cleanup_child_wrapper_on_signal' TERM INT HUP
	exec 3<&0 || return 1

	# Preserve the wrapper's stdin for background children. Some /bin/sh
	# implementations reattach asynchronous jobs to /dev/null unless stdin is
	# duplicated onto a dedicated descriptor before the background launch.
	sh -c "$l_cleanup_wrapper_exec_cmd" <&3 &
	l_cleanup_wrapper_child_pid=$!
	wait "$l_cleanup_wrapper_child_pid"
	l_cleanup_wrapper_status=$?
	exec 3<&-
	return "$l_cleanup_wrapper_status"
}

if [ "${ZXFER_CLEANUP_CHILD_WRAPPER_SOURCE_ONLY:-0}" != "1" ]; then
	zxfer_cleanup_child_wrapper_main "$@"
	exit $?
fi
