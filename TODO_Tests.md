# TODO: Integration Tests for zxfer

Current integration coverage in `tests/integration_zxfer.sh` focuses on local replication happy paths and a handful of CLI validation guards. The codebase includes many option-driven branches that are still untested end-to-end. Add the following higher-level tests to reach coverage across every path:

## CLI and Option Validation
- [x] `zxfer:133-150`, `src/zxfer_zfs_mode.sh:279-288` – invoke `zxfer` without a destination, with missing `-N/-R`, and with both flags simultaneously to verify `throw_usage_error` halts and prints usage. (Covered by `usage_error_tests` in `tests/integration_zxfer.sh`.)  
- [x] `src/zxfer_zfs_mode.sh:297-305` – run with sources/destinations that start with `/` and with a snapshot source (`pool/fs@snap`) to cover `check_snapshot` and path validation errors.  
- [x] `src/zxfer_zfs_mode.sh:307-311` – pass `-c` without `-m` to ensure migration guard fails.  
- [ ] `src/zxfer_globals.sh:473-499` – cover each `consistency_check` branch: `-k` with `-e`, `-b` with `-B`, `-z` without `-O/-T`, and `-m`/`-c` with remote hosts. (First three covered by `consistency_option_validation_tests`; remote combination still outstanding.)  
- [ ] `src/zxfer_transfer_properties.sh:283-296` – supply an invalid `-o property=value` to ensure option validation rejects unknown properties.  

## Dataset Selection & Snapshot Discovery
- [ ] `src/zxfer_zfs_mode.sh:51-72` – replicate once with a trailing slash on the source and once without to verify `set_actual_dest` either creates child datasets or writes directly into the destination root.  
- [x] `src/zxfer_zfs_mode.sh:279-334` – add a non-recursive `-N` test to prove child datasets are not touched, and another with `-R` to ensure recursion still works when child datasets share parents. (`non_recursive_replication_test` now covers the `-N` path alongside existing recursive coverage.)  
- [ ] `src/zxfer_get_zfs_list.sh:172-196` – exercise the `-x` exclude filter so datasets matching the pattern are removed from `g_recursive_source_list`.  
- [ ] `src/zxfer_get_zfs_list.sh:45-113` – run with `-j 1` and `-j >1` to hit both code paths in `write_source_snapshot_list_to_file`, including the remote/`zstd` compression branch by running with `-O localhost -z`.  
- [ ] `src/zxfer_get_zfs_list.sh:108-165` – cover the “destination dataset missing” path so the stripped snapshot list is empty and `g_recursive_source_list` only reflects sources with new snapshots.  
- [ ] `src/zxfer_get_zfs_list.sh:201-244` – force `g_recursive_source_list` to be empty (no new snapshots) and assert the integration run exits cleanly without touching send/receive to cover `comm -23` and the V-verbose diagnostics.  
- [ ] `src/zxfer_zfs_mode.sh:332-341` – run with `-s` (once with `-R` and once with `-N`) so the non-migration snapshot branch in `newsnap` executes, proving the auto-created snapshot is replicated. (`auto_snapshot_replication_test` covers the `-R` branch; `-N` coverage still needed.)  

## Snapshot Transfer & Send/Receive Pipeline
- [ ] `src/zxfer_zfs_mode.sh:223-273` – craft a dataset with no pending snapshots to ensure `copy_snapshots` returns early; also cover the branch where the destination dataset does not yet exist so the first snapshot is sent synchronously.  
- [ ] `src/zxfer_zfs_send_receive.sh:100-123` – verify `get_send_command` behavior for initial full sends, incremental sends, `-v`, and `-w` (raw) flags by observing the constructed command via a dry-run wrapper.  
- [ ] `src/zxfer_zfs_send_receive.sh:124-147` – supply `-F` during a run where the destination has diverged to confirm the receive command includes `-F` and actually rolls the target back.  
- [ ] `src/zxfer_zfs_send_receive.sh:150-200` – add a test with `-j 3` to prove background sends respect the job limit (no more than `-j` concurrent `zfs send` processes) and another to ensure the `l_is_allow_background=0` path is hit for the first snapshot.  
- [ ] `src/zxfer_zfs_send_receive.sh:44-89` – run with `-D 'pv -s %%size%% > /dev/null'` so `calculate_size_estimate` and `handle_progress_bar_option` wrap the send stream without breaking dataset creation.  
- [ ] `src/zxfer_zfs_send_receive.sh:129-176` – cover local, `-O`, `-T`, and combined `-O/-T` transfers, both with and without `-z/-Z`, to exercise `wrap_command_with_ssh` compression logic and ensure the decompression half on receive works.  
- [ ] `src/zxfer_zfs_mode.sh:404-437` – enable `-Y` so the loop repeats at least twice: one run where `g_is_performed_send_destroy` stays `1` (due to `-d` or a pending send) and another where it drops to `0` to exit.  

## Snapshot Deletion & Retention
- [ ] `src/zxfer_inspect_delete_snap.sh:159-215` – replicate, remove a snapshot on the source, rerun with `-d` to ensure `delete_snaps` destroys extra destination snapshots and toggles `g_is_performed_send_destroy`.  
- [ ] `src/zxfer_inspect_delete_snap.sh:199-207` – repeat the deletion scenario with `-n` to cover the dry-run guard that prints the destroy command without executing.  
- [ ] `src/zxfer_inspect_delete_snap.sh:123-155` – set `-g 0` while trying to delete a snapshot so `grandfather_test` triggers the protection error path.  
- [ ] `src/zxfer_inspect_delete_snap.sh:47-71` – induce mismatched source/destination snapshot names to validate `get_dest_snapshots_to_delete_per_dataset`’s background sorting logic (ensure both temp files are consumed).  

## Property Replication, Backup, and Overrides
- [ ] `src/zxfer_transfer_properties.sh:222-446` – add a test where the destination does not exist and `-P` is set so `transfer_properties` creates it with inherited properties; include a child dataset case to hit the creation branch that uses `-p` and `creation_pvs`.  
- [ ] `src/zxfer_transfer_properties.sh:445-669` – add a scenario where the destination already exists with mismatched properties to force the “must create” validation error, the `zfs set` path, and the `zfs inherit` path.  
- [ ] `src/zxfer_transfer_properties.sh:560-619` – test `-o` overrides on both the initial source and a child so we cover `ov_initsrc_set_list` collapsing into `ov_set_list`.  
- [ ] `src/zxfer_transfer_properties.sh:549-555` – use `-I mountpoint,compression` (or similar) to validate that ignored properties are skipped for both creation and override lists.  
- [ ] `src/zxfer_zfs_mode.sh:200-221` and `src/zxfer_transfer_properties.sh:530-555` – run with `-U` while presenting a destination that pretends not to support one property (via a wrapper `zfs` binary) so that `calculate_unsupported_properties` builds `unsupported_properties` and `remove_unsupported_properties` logs the skip.  
- [ ] `src/zxfer_globals.sh:505-544` & `src/zxfer_transfer_properties.sh:250-266` – add `-k` backup coverage: run a replication that writes the backup file, inspect it for the expected header, and then run a follow-up `-e` restore that pulls the saved properties from an ancestor directory.  
- [ ] `src/zxfer_transfer_properties.sh:369-377` – perform a migration (`-m`) to ensure `mountpoint` is removed from the read-only list and verify the target inherits the original mountpoint.  
- [ ] `src/zxfer_transfer_properties.sh:235-238` – replicate a zvol so the `-V <volsize>` branch is exercised during destination creation.  
- [ ] `src/zxfer_transfer_properties.sh:505-533` – purposely create a destination with conflicting `casesensitivity`/`normalization` to confirm the “must create” properties error path is triggered.  
- [ ] (Unreachable today) Document that `g_ensure_writable` is always `0`; add a test or a follow-up issue to expose a CLI switch that allows toggling it so the `readonly=off` rewrite can be exercised.  

## Migration & Service Handling
- [ ] `src/zxfer_zfs_mode.sh:333-377` – write a migration test (`-m`) that snapshots, unmounts, replicates, transfers mountpoints, and remounts, verifying services restart and the old filesystems stay unmounted.  
- [ ] `src/zxfer_zfs_mode.sh:333-377` & `src/zxfer_zfs_mode.sh:348-369` – extend the migration test to use `-c svc:/system/filesystem/local` (or a fake SMF stub) so `stopsvcs` and `relaunch` are exercised, including the error branch if `svcadm disable` fails.  
- [ ] `src/zxfer_zfs_mode.sh:233-245` – attempt `-m` against a deliberately unmounted dataset to hit the guard that aborts migrations when the source is not mounted.  

## Remote Operations, Compression, and Cleanup
- [ ] `src/zxfer_globals.sh:164-220` & `src/zxfer_globals.sh:193-238` – run with `-O localhost` and `-T localhost` (using SSH control sockets) to confirm `setup_ssh_control_socket` creates a per-role socket, reuses it, and that `close_*` removes the socket directories at exit.  
- [ ] `src/zxfer_globals.sh:250-287` – send `SIGINT` to a running integration test (e.g., while a large transfer is sleeping) to cover `trap_exit`, ensuring background jobs are killed, temp files deleted, and SSH sockets closed.  
- [ ] `src/zxfer_zfs_send_receive.sh:129-176` & `src/zxfer_get_zfs_list.sh:45-113` – cover the remote snapshot listing/compression path (`-O`, `-j>1`, `-z`) and the remote receive compression path (`-T`, `-Z 'zstd -T0 -6'`).  
- [ ] `src/zxfer_globals.sh:355-382` – verify that `get_os` resolves correctly for both local and remote hosts (mock `uname` responses) so platform-specific property filters (`g_fbsd_readonly_properties`, `g_solexp_readonly_properties`) engage.  

## Dry Run, Logging, and Audible Alerts
- [ ] `src/zxfer_common.sh:126-144` & `src/zxfer_inspect_delete_snap.sh:199-207` – create a single test that runs `zxfer -n` through a typical replication and another with `-n -d` to show both send and destroy commands are printed instead of executed.  
- [ ] `src/zxfer_common.sh:188-219` – run with `-v` and `-V` to assert verbose output and debug logging appear; capture stderr to ensure `echoV` emits to stderr only.  
- [ ] `src/zxfer_common.sh:203-220` – (platform-dependent) add a FreeBSD-only test that sets `-b` and `-B` separately, confirming `beep` loads `speaker.ko` once and writes to `/dev/speaker`, and fails gracefully when the module is missing.  

## Failure Handling
- [ ] `src/zxfer_get_zfs_list.sh:312-317` – feed zxfer a nonexistent source dataset to trigger “Failed to retrieve snapshots from the source,” and a nonexistent destination root to trigger “Failed to retrieve list of datasets from the destination,” ensuring both fatal error paths are covered.  

Document the intent, preconditions (e.g., need for GNU parallel, SSH keys, SMF availability, ability to create ZFS pools backed by sparse files), and expected assertions inside each new test case so they can be implemented incrementally in `tests/integration_zxfer.sh` (or companion scripts) without destabilizing existing scenarios.
