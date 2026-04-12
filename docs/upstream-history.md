# Upstream History

zxfer began as a long-lived shell script for ZFS replication. This fork
continues that work with an emphasis on current OpenZFS platforms, safer
operation, and maintainable documentation.

## Branch Guide

The repository keeps three long-lived branches for different purposes:

- `main`: the active development branch for this fork. New features, fixes,
  refactors, and documentation work merge here.
- `upstream-compat-final`: a historical reference point from this fork before
  rsync-mode removal and before the later breaking divergence on `main`. It
  represents the forked codebase while it was still closer to the older
  behavior and still carried the rsync-capable path.
- `upstream-archive`: a reference branch containing the latest imported history from
  [allanjude/zxfer](https://github.com/allanjude/zxfer). It exists to make the
  upstream codebase easy to inspect without leaving this repository.

Only `main` is the active branch for ongoing development.
`upstream-compat-final` and `upstream-archive` are reference branches, not the
place where new work lands.

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
Within this repository, `upstream-compat-final` is the closest branch-level
reference for the fork before rsync removal, while `upstream-archive` is the
easiest way to inspect the latest imported upstream branch.

## Where To Find Current Guidance

For current usage and maintenance guidance, start with:

- [../README.md](../README.md) for the project overview and quick start
- [whats-new-since-v1.1.7.md](./whats-new-since-v1.1.7.md) for an upgrade-oriented summary of the changes from the legacy 2019 release
- [../man/zxfer.8](../man/zxfer.8) and [../man/zxfer.1m](../man/zxfer.1m) for the full CLI reference
- [testing.md](./testing.md), [platforms.md](./platforms.md), and [troubleshooting.md](./troubleshooting.md) for operational details
