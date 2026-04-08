# Architecture

## Entry Point

- [../zxfer](../zxfer): top-level launcher and CLI entry point

The entry script sources the shell modules under `src/` and drives the overall
replication flow.

## Module Layout

- [../src/zxfer_common.sh](../src/zxfer_common.sh): shared helpers, secure-path
  handling, quoting, temp files, failure reporting
- [../src/zxfer_globals.sh](../src/zxfer_globals.sh): global initialization,
  CLI parsing, local dependency resolution, runtime state
- [../src/zxfer_secure_paths.sh](../src/zxfer_secure_paths.sh): filesystem
  ownership/mode checks, secure-path validation, symlink-aware path guards
- [../src/zxfer_remote_cli.sh](../src/zxfer_remote_cli.sh): remote helper
  resolution, capability handshakes, ssh control-socket management
- [../src/zxfer_backup_metadata.sh](../src/zxfer_backup_metadata.sh): backup
  metadata path derivation, secure lookup, legacy fallback, read/write flows
- [../src/zxfer_property_cache.sh](../src/zxfer_property_cache.sh): normalized
  property caching, prefetch state, iteration cache invalidation
- [../src/zxfer_get_zfs_list.sh](../src/zxfer_get_zfs_list.sh): source and
  destination dataset / snapshot discovery
- [../src/zxfer_inspect_delete_snap.sh](../src/zxfer_inspect_delete_snap.sh):
  snapshot comparison and deletion planning
- [../src/zxfer_transfer_properties.sh](../src/zxfer_transfer_properties.sh):
  property diffing, filtering, override planning, and apply logic
- [../src/zxfer_zfs_send_receive.sh](../src/zxfer_zfs_send_receive.sh): send /
  receive command construction, progress pipeline, compression handling
- [../src/zxfer_zfs_mode.sh](../src/zxfer_zfs_mode.sh): dataset iteration,
  replication orchestration, migration/service handling

## High-Level Replication Flow

1. Parse CLI options and initialize secure tool paths.
2. Resolve source and destination execution context.
3. Build dataset and snapshot lists.
4. Inspect source versus destination state.
5. Optionally delete destination-only snapshots.
6. Optionally transfer or restore properties.
7. Send and receive snapshots.
8. Repeat when `-Y` is enabled.
9. Emit structured failure reporting on non-zero exit.

## Design Priorities

The project is organized around:

- safety before throughput
- security before convenience
- testability of shell helpers
- portability across ZFS platforms

## Documentation Sources Of Truth

- man pages for the complete CLI reference
- `README.md` for the top-level overview and quick start
- `docs/` for operational and contributor guidance
- `KNOWN_ISSUES.md` for current limitations
