# Architecture

## Entry Point

- [../zxfer](../zxfer): top-level launcher and CLI entry point

The entry script sources the shell modules under `src/` and drives the overall
replication flow.

## Module Layout

- [../src/zxfer_common.sh](../src/zxfer_common.sh): shared helpers, secure-path
  handling, quoting, ssh helpers, temp files, failure reporting
- [../src/zxfer_globals.sh](../src/zxfer_globals.sh): global initialization,
  CLI parsing, dependency resolution, backup metadata lookup
- [../src/zxfer_get_zfs_list.sh](../src/zxfer_get_zfs_list.sh): source and
  destination dataset / snapshot discovery
- [../src/zxfer_inspect_delete_snap.sh](../src/zxfer_inspect_delete_snap.sh):
  snapshot comparison and deletion planning
- [../src/zxfer_transfer_properties.sh](../src/zxfer_transfer_properties.sh):
  property collection, filtering, diffing, and apply logic
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
