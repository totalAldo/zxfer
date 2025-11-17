# TODO – Logical Bugs

## src/zxfer_zfs_mode.sh
- [ ] `stopsvcs()` (src/zxfer_zfs_mode.sh:132-141) reads the entire `-c` string as a single service because the list is whitespace-separated but `read -r service` only iterates by newline. Supplying multiple FMRI patterns causes `svcadm disable` to receive one concatenated argument and fail. Split the list into individual services (e.g., convert spaces to newlines before piping) before disabling them.

## src/zxfer_zfs_send_receive.sh
- [ ] The progress bar hook (`handle_progress_bar_option` at src/zxfer_zfs_send_receive.sh:71-88) appends `| dd … | dd … | $g_option_D_display_progress_bar` directly to the send stream but never tees the data back to `zfs receive`. Any progress command that does not echo the stream (e.g., dialog gauges) consumes the data, so the receive side sees an incomplete stream and new filesystem creates fail with `cannot receive … incomplete stream`. The hook needs a tee or pv-style command that passes the bytes through.
- [ ] When `-j` launches multiple send/receive jobs, `zfs_send_receive()` backgrounds `execute_command` (src/zxfer_zfs_send_receive.sh:134-166). Any failure inside the background subshell only exits that job; the parent never observes the error because the trailing `wait` in `copy_filesystems()` ignores exit statuses. Replication errors are silently swallowed, so datasets can remain stale even though zxfer reports success. Capture background PIDs and fail fast when any returns non-zero.
