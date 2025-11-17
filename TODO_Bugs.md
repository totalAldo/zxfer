# TODO – Logical Bugs

## src/zxfer_inspect_delete_snap.sh
- [x] `get_last_common_snapshot()` (src/zxfer_inspect_delete_snap.sh:77-109) searches for the snapshot suffix using `grep -qF "$l_snap_name"`. Because the destination list still contains dataset prefixes (`pool/fs@snap`), a snapshot named `snap1` will incorrectly match `snap10`, and the code will try to resume from a snapshot the destination does not have. Anchor the match to `@${name}$` or strip dataset prefixes before the comparison.

## src/zxfer_transfer_properties.sh
- [ ] When restoring properties from a backup (`g_option_e_restore_property_mode`), the script builds a `grep` regex with the raw dataset name (src/zxfer_transfer_properties.sh:258-260). Datasets with dots or other regex metacharacters match the wrong line, so properties from a different filesystem can be applied silently. Escape the dataset string before constructing the regex.

## src/zxfer_zfs_mode.sh
- [ ] `copy_snapshots()` (src/zxfer_zfs_mode.sh:103-125) only performs a full send when the destination dataset is missing. If the dataset already exists but has zero snapshots, `g_last_common_snap` stays empty and the code sends **only the most recent snapshot**, skipping the older history entirely. Even first-time replicas into pre-created datasets lose all earlier snapshots; the function must seed the destination with the first snapshot even when the filesystem already exists.
- [ ] `stopsvcs()` (src/zxfer_zfs_mode.sh:132-141) reads the entire `-c` string as a single service because the list is whitespace-separated but `read -r service` only iterates by newline. Supplying multiple FMRI patterns causes `svcadm disable` to receive one concatenated argument and fail. Split the list into individual services (e.g., convert spaces to newlines before piping) before disabling them.

## src/zxfer_zfs_send_receive.sh
- [ ] The progress bar hook (`handle_progress_bar_option` at src/zxfer_zfs_send_receive.sh:71-88) appends `| dd … | dd … | $g_option_D_display_progress_bar` directly to the send stream but never tees the data back to `zfs receive`. Any progress command that does not echo the stream (e.g., dialog gauges) consumes the data, so the receive side sees an incomplete stream and new filesystem creates fail with `cannot receive … incomplete stream`. The hook needs a tee or pv-style command that passes the bytes through.
- [ ] When `-j` launches multiple send/receive jobs, `zfs_send_receive()` backgrounds `execute_command` (src/zxfer_zfs_send_receive.sh:134-166). Any failure inside the background subshell only exits that job; the parent never observes the error because the trailing `wait` in `copy_filesystems()` ignores exit statuses. Replication errors are silently swallowed, so datasets can remain stale even though zxfer reports success. Capture background PIDs and fail fast when any returns non-zero.

## tests/integration_zxfer.sh
- [ ] `parallel_jobs_test()` is defined (tests/integration_zxfer.sh:255-295) but never executed because the call is commented out in `main()` (line 337). As a result, concurrency regressions in `-j` mode go undetected despite having a test. Re-enable the call (gating on GNU parallel availability) so CI exercises the parallel send path.
