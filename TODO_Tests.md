# TODO: Integration Tests for zxfer

Current integration coverage in `tests/integration_zxfer.sh` focuses on local replication happy paths and a handful of CLI validation guards. The codebase includes many option-driven branches that are still untested end-to-end. Add the following higher-level tests to reach coverage across every path:

## Snapshot Transfer & Send/Receive Pipeline

- [ ] `src/zxfer_zfs_send_receive.sh:187-214` & `src/zxfer_zfs_send_receive.sh:262-274` – cover local, `-O`, `-T`, and combined `-O/-T` transfers, both with and without `-z/-Z`, to exercise `wrap_command_with_ssh` compression logic and ensure the decompression half on receive works.

## Snapshot Deletion & Retention

- [ ] `src/zxfer_inspect_delete_snap.sh:47-71` – induce mismatched source/destination snapshot names to validate `get_dest_snapshots_to_delete_per_dataset`’s background sorting logic (ensure both temp files are consumed).

## Property Replication, Backup, and Overrides

- [ ] `src/zxfer_transfer_properties.sh:215-254` & `src/zxfer_transfer_properties.sh:497-535` – add a test where the destination does not exist and `-P` is set so `transfer_properties` creates it with inherited properties; include a child dataset and a zvol case to hit the `-p` and `-V <volsize>` branches.
- [ ] `src/zxfer_transfer_properties.sh:600-739` & `src/zxfer_transfer_properties.sh:775-813` – add a scenario where the destination already exists with mismatched properties to force the “must create” validation error, the `zfs set` path, and the `zfs inherit` path.
- [ ] `src/zxfer_transfer_properties.sh:377-430` & `src/zxfer_transfer_properties.sh:785-813` – test `-o` overrides on both the initial source and a child so we cover `ov_initsrc_set_list` collapsing into `ov_set_list`, even when no new snapshots are pending.
- [ ] `src/zxfer_transfer_properties.sh:438-464` & `src/zxfer_transfer_properties.sh:791-803` – use `-I mountpoint,compression` (or similar) to validate that ignored properties are skipped for both creation and override lists.
- [ ] `src/zxfer_zfs_mode.sh:214-248` and `src/zxfer_transfer_properties.sh:471-482, 784-793` – run with `-U` while presenting a destination that pretends not to support one property (via a wrapper `zfs` binary) so that `calculate_unsupported_properties` builds `unsupported_properties` and `strip_unsupported_properties` logs the skip.
- [ ] `src/zxfer_transfer_properties.sh:804-813` & `src/zxfer_zfs_mode.sh:253-279` – cover the property-only pass when snapshots are already synchronized (`-P/-o/-k` with no pending sends) to ensure the forced dataset iteration still applies or inherits properties.
- [ ] `src/zxfer_zfs_mode.sh:412-441` & `src/zxfer_transfer_properties.sh:804-813` – perform a migration (`-m`) to ensure `mountpoint` is removed from the read-only list and verify the target inherits the original mountpoint.
- [ ] `src/zxfer_transfer_properties.sh:497-535` & `src/zxfer_transfer_properties.sh:804-813` – purposely create a destination with conflicting `casesensitivity`/`normalization` to confirm the “must create” properties error path is triggered.
- [ ] (Unreachable today) Document that `g_ensure_writable` is always `0`; add a test or a follow-up issue to expose a CLI switch that allows toggling it so the `readonly=off` rewrite can be exercised.

## Migration & Service Handling

- [ ] `src/zxfer_zfs_mode.sh:412-473` – write a migration test (`-m`) that snapshots, unmounts, replicates, transfers mountpoints, and remounts, verifying services restart and the old filesystems stay unmounted.
- [ ] `src/zxfer_zfs_mode.sh:137-176` & `src/zxfer_zfs_mode.sh:412-441` – extend the migration test to use `-c svc:/system/filesystem/local` (or a fake SMF stub) so `stopsvcs` and `relaunch` are exercised, including the error branch if `svcadm disable` fails.

## Remote Operations, Compression, and Cleanup

- [ ] `src/zxfer_globals.sh:288-327` & `src/zxfer_globals.sh:329-402` – run with `-O localhost` and `-T localhost` (using SSH control sockets) to confirm `setup_ssh_control_socket` creates a per-role socket, reuses it, and that `close_*` removes the socket directories at exit.
- [ ] `src/zxfer_globals.sh:404-444` – send `SIGINT` to a running integration test (e.g., while a large transfer is sleeping) to cover `trap_exit`, ensuring background jobs are killed, temp files deleted, and SSH sockets closed.
- [ ] `src/zxfer_get_zfs_list.sh:88-120` & `src/zxfer_zfs_send_receive.sh:187-214` – cover the remote snapshot listing/compression path (`-O`, `-j>1`, `-z`) and the remote receive compression path (`-T`, `-Z 'zstd -T0 -6'`).
- [ ] `src/zxfer_common.sh:80-93` & `src/zxfer_globals.sh:606-651` – verify that `get_os` resolves correctly for both local and remote hosts (mock `uname` responses) so platform-specific property filters (`g_fbsd_readonly_properties`, `g_solexp_readonly_properties`) engage.

## Dry Run, Logging, and Audible Alerts

- [ ] `src/zxfer_common.sh:403-416` – run with `-v` and `-V` to assert verbose output and debug logging appear; capture stderr to ensure `echoV` emits to stderr only.
- [ ] `src/zxfer_common.sh:418-447` – (platform-dependent) add a FreeBSD-only test that sets `-b` and `-B` separately, confirming `beep` loads `speaker.ko` once and writes to `/dev/speaker`, and fails gracefully when the module is missing.
