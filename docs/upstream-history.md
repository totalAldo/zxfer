# Upstream History

zxfer began as a long-lived shell script for ZFS replication. This fork
continues that work with an emphasis on current OpenZFS platforms, safer
operation, and maintainable documentation.

## What This Fork Focuses On

Compared with the older upstream project, this fork emphasizes:

- safer `zfs send` / `zfs receive` replication
- stronger test coverage
- secure PATH resolution
- structured failure reporting
- safer backup metadata handling
- better remote-host portability

## Removed Legacy Behavior

The legacy rsync mode (`-S`) from the older upstream project has been removed.
This fork focuses on ZFS-native replication only.

Legacy backup-metadata restore compatibility has also been removed. Current
`-e` restores require the hardened source-dataset-relative keyed files written
by current `-k` runs under `ZXFER_BACKUP_DIR`.

If you depended on rsync-style behavior, use an older release or another tool.

## Where To Find Current Guidance

For current usage and maintenance guidance, start with:

- [../README.md](../README.md) for the project overview and quick start
- [../man/zxfer.8](../man/zxfer.8) and [../man/zxfer.1m](../man/zxfer.1m) for the full CLI reference
- [testing.md](./testing.md), [platforms.md](./platforms.md), and [troubleshooting.md](./troubleshooting.md) for operational details
