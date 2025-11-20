# TODO – Logical Bugs

- `beep()` returns early when `/dev/speaker` is absent, so it never tries to `kldload` `speaker.ko` and the `-b`/`-B` flags stay silent unless the module is preloaded (src/zxfer_common.sh:428-452).
- When creating a missing top-level destination dataset, `ensure_destination_exists()` applies every source property (including inherited ones) and returns before running the inherit/set diff, pinning inherited properties as local on the target and breaking future inheritance from the destination pool (src/zxfer_transfer_properties.sh:520-535).
- The `-g` days flag is not validated; non-numeric input reaches the arithmetic check in `grandfather_test()` and triggers a shell error instead of a friendly usage failure (src/zxfer_globals.sh:515-517, src/zxfer_inspect_delete_snap.sh:131-161).


# TODO – Security Review

- Remote helper calls (e.g., `get_os` and remote snapshot listing/compression) run under the remote account’s default `PATH`, so a compromised or untrusted login shell can swap binaries like `uname`, `parallel`, or `zstd` instead of zxfer’s allowlisted paths (src/zxfer_common.sh:80-90, src/zxfer_get_zfs_list.sh:65-109). Propagate the secure `PATH` to ssh commands or resolve absolute paths remotely to avoid trojan execution.
