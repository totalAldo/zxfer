# TODO – Logical Bugs

- [ ] Destination paths with a trailing slash are never normalized, so `set_actual_dest()` (`src/zxfer_zfs_mode.sh:51-72`) concatenates `$g_destination` and the child dataset with an extra `/`. Invocations such as `zxfer -N tank/src backup/dst/` therefore generate targets like `backup/dst//src`, which `zfs` rejects as invalid dataset names. The source path is stripped of trailing slashes in `run_zfs_mode()` (`src/zxfer_zfs_mode.sh:316-325`); the destination should be normalized the same way before replication starts.
