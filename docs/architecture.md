# Architecture

## Entry Point

- [../zxfer](../zxfer): top-level launcher and CLI entry point

The entry script now sources only
[../src/zxfer_modules.sh](../src/zxfer_modules.sh). That loader owns runtime
module order for the launcher, `tests/test_helper.sh`, and other
direct-sourcing fixtures, so the flat `src/` layout keeps one canonical source
sequence.

## Module Layout

The `src/` tree remains flat, but each file now owns a stable long-term
responsibility boundary.

- [../src/zxfer_modules.sh](../src/zxfer_modules.sh): canonical loader and
  source-order entry point for the runtime modules
- [../src/zxfer_reporting.sh](../src/zxfer_reporting.sh): structured failure
  reporting, verbose output helpers, usage errors, and operator-facing status
- [../src/zxfer_exec.sh](../src/zxfer_exec.sh): shell-safe token handling,
  command rendering, ssh wrappers, and exec helpers
- [../src/zxfer_dependencies.sh](../src/zxfer_dependencies.sh): secure PATH
  computation, required-tool lookup, and local dependency validation
- [../src/zxfer_runtime.sh](../src/zxfer_runtime.sh): runtime/session
  initialization, shared per-run defaults, temp-file lifecycle, cleanup
  registration, and trap handling
- [../src/zxfer_cli.sh](../src/zxfer_cli.sh): CLI parsing, option validation,
  and compression command interpretation
- [../src/zxfer_snapshot_state.sh](../src/zxfer_snapshot_state.sh): snapshot
  record parsing, normalization, and cached snapshot index state
- [../src/zxfer_path_security.sh](../src/zxfer_path_security.sh): filesystem
  ownership/mode checks, secure-path validation, symlink-aware path guards
- [../src/zxfer_remote_hosts.sh](../src/zxfer_remote_hosts.sh): remote helper
  resolution, capability handshakes, ssh control-socket management
- [../src/zxfer_backup_metadata.sh](../src/zxfer_backup_metadata.sh): backup
  metadata accumulation, path derivation, and secure exact-keyed lookup/read/write flows
- [../src/zxfer_property_cache.sh](../src/zxfer_property_cache.sh): normalized
  property caching, prefetch state, startup/iteration cache reset helpers
- [../src/zxfer_snapshot_discovery.sh](../src/zxfer_snapshot_discovery.sh):
  source and destination dataset / snapshot discovery
- [../src/zxfer_snapshot_reconcile.sh](../src/zxfer_snapshot_reconcile.sh):
  snapshot comparison and deletion planning
- [../src/zxfer_property_reconcile.sh](../src/zxfer_property_reconcile.sh):
  readonly-property defaults, unsupported-property derivation, property
  diffing, filtering, override planning, per-call scratch resets, and apply
  logic
- [../src/zxfer_send_receive.sh](../src/zxfer_send_receive.sh): send /
  receive command construction, progress pipeline, compression handling
- [../src/zxfer_replication.sh](../src/zxfer_replication.sh): dataset iteration,
  replication orchestration, migration/service handling

## Initialization And State Ownership

The startup path is intentionally explicit:

1. [../src/zxfer_modules.sh](../src/zxfer_modules.sh) loads the flat module
   stack in one canonical order.
2. `zxfer_init_globals()` seeds generic runtime/session state in
   [../src/zxfer_runtime.sh](../src/zxfer_runtime.sh).
3. Module-specific mutable scratch state is then reset through the owning
   module helpers rather than by duplicating those variable inventories in the
   runtime layer. The main examples are
   [../src/zxfer_snapshot_discovery.sh](../src/zxfer_snapshot_discovery.sh),
   [../src/zxfer_snapshot_reconcile.sh](../src/zxfer_snapshot_reconcile.sh),
   [../src/zxfer_send_receive.sh](../src/zxfer_send_receive.sh),
   [../src/zxfer_backup_metadata.sh](../src/zxfer_backup_metadata.sh),
   [../src/zxfer_property_cache.sh](../src/zxfer_property_cache.sh), and
   [../src/zxfer_property_reconcile.sh](../src/zxfer_property_reconcile.sh).
4. `zxfer_init_variables()` resolves local/remote execution context, helper
   paths, and platform-specific bootstrap details.

That split keeps startup readable without reintroducing source-time side
effects or generic catch-all modules.

## High-Level Replication Flow

1. Parse CLI options and initialize secure tool paths.
2. Load the flat module stack, initialize runtime/session state through the
   explicit init flow, then resolve source and destination execution context.
3. Build dataset and snapshot lists.
4. Inspect source versus destination state.
5. Optionally delete destination-only snapshots.
6. Transfer snapshots through explicit stage helpers:
   live recheck, seed decision, then final send/receive range. Seed-only
   receive `-F` is passed as an internal execution flag without mutating the
   parsed `g_option_*` state.
7. Optionally transfer or restore properties, including exact-keyed backup
   metadata reads and deferred post-seed reconciliation for datasets that were
   seeded into empty destinations.
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
