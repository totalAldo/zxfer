# TODO – Logical Bugs

## src/zxfer_zfs_send_receive.sh
- [ ] When `-j` launches multiple send/receive jobs, `zfs_send_receive()` backgrounds `execute_command` (src/zxfer_zfs_send_receive.sh:134-166). Any failure inside the background subshell only exits that job; the parent never observes the error because the trailing `wait` in `copy_filesystems()` ignores exit statuses. Replication errors are silently swallowed, so datasets can remain stale even though zxfer reports success. Capture background PIDs and fail fast when any returns non-zero.
