# Optimization Review

This document catalogs the current performance baseline in the `zxfer`
codebase. The larger discovery, caching, delete-path, property-path,
progress-planning, and remote-startup optimizations are already landed, so
this file now serves primarily as a compact baseline and measurement reference
for any future profiling-driven work. It focuses on four areas the project
already cares about:

- caching repeated work
- minimizing `zfs` and `ssh` calls
- improving concurrency behavior
- replacing shell paths that scale poorly on large dataset/snapshot trees

This is a review document only. No behavior changes are proposed here without
separate implementation, tests, and manual integration validation.

## Existing Strengths

The current tree already contains several good performance-oriented choices:

- snapshot tree diffing for recursive replication already relies on sorted lists
  plus `comm` instead of older nested-loop comparison patterns
- `zfs send -I` is used so incremental chains move in one stream
- source snapshot discovery now adapts between a single recursive list and the
  older per-dataset GNU `parallel` fan-out based on dataset count plus remote
  startup warmth, validates GNU `parallel` only when the chosen branch
  actually needs it, and reuses the prefetched dataset list directly inside the
  selected command path, so startup-bound and externally orchestrated `-j` runs
  no longer always pay the full N+1 discovery cost before data moves
- remote startup discovery now collapses `uname` plus helper-path lookups into
  one per-host capability handshake, with current-process reuse and a
  short-lived cross-process cache stored in a validated per-user `0700`
  directory under `TMPDIR`
- concurrent sibling zxfer processes now coalesce remote capability handshakes
  through a secure per-host lock, so a burst of same-host processes reuses one
  live probe result instead of stampeding the helper-discovery ssh path before
  the cache is populated
- ssh control sockets are now reused across sibling zxfer processes through a
  validated per-user cache directory under `TMPDIR`, with per-process lease
  files and stale-lease pruning so one process can reuse another process's
  live transport without tearing it down out from under concurrent siblings
- `-j` send/receive scheduling now keeps a rolling background worker pool full
  instead of waiting for an entire batch to drain before launching the next
  transfer
- common-snapshot selection and live destination rechecks already compare
  snapshot identities using `name+guid` records instead of trusting snapshot
  names alone
- common-snapshot selection and live destination rechecks now use one-pass
  `awk`-backed identity-set lookups instead of repeated shell-string
  membership scans
- recursive snapshot discovery now keeps the raw global source/destination
  snapshot caches and lazily builds the per-dataset source/destination
  snapshot indexes only when delete/common-snapshot planning actually asks for
  per-dataset records, so no-op runs no longer pay the reverse/index build
  cost up front while delete/common-snapshot paths still consume pre-sliced
  records instead of re-filtering the full global snapshot lists for every
  dataset pass
- recursive snapshot discovery now starts with name-only source and
  destination snapshot records plus name-only per-dataset indexes, and only
  fetches guid-bearing records for datasets whose overlapping snapshot names
  actually require common-snapshot, delete, rollback, or live-recheck
  identity validation
- recursive `-d` runs now track datasets with destination-only snapshot deltas
  separately and iterate only datasets with source deltas, destination-only
  deltas, or explicit property-reconcile work, so no-op recursive delete runs
  no longer force every dataset through `get_last_common_snapshot()`,
  delete-planning, and copy-path setup just because `-d` is enabled
- per-dataset property reconciliation now batches consecutive `zfs set`
  operations instead of issuing one destination-side call per property
- normalized property reads and explicit required-property probes are now cached
  per replication iteration with destination-side invalidation after create,
  receive, set, and inherit operations
- recursive property-transfer runs now prefetch source and destination property
  trees once per iteration, slice normalized per-dataset results locally, and
  reuse prefetched destination parent state for child-inherit adjustment before
  falling back to exact live reads for datasets created or mutated mid-iteration
- the remaining small property-diff helper loops now use `awk`-backed
  transforms for override validation, override/create-list derivation,
  destination diffing, child inherit adjustment, and unsupported-property
  filtering instead of repeatedly spawning `cut`, `grep`, and shell nested-loop
  scans across the same short property lists
- destination-only snapshot deletion planning now resolves identity-to-path
  matches in one destination-order-preserving pass instead of rescanning the
  full destination snapshot list for every delete candidate
- rollback-eligibility and grandfather checks now batch per-dataset snapshot
  `creation` reads and cache the numeric results locally, so delete-heavy
  runs no longer pay one remote `zfs get creation` round trip per candidate
  snapshot before the final human-readable grandfather error path
- destination existence probes now fail closed on operational errors instead of
  treating every failed `zfs list` as proof that the dataset is absent
- destination existence answers are now cached per replication iteration from
  the recursive destination dataset list and updated after successful creates
  and foreground receives, reducing redundant `zfs list -H` probes on non-
  destructive paths while keeping live rechecks on safety-critical branches
- deferred post-seed property reconciliation now updates seeded destination
  datasets incrementally and clears only destination-side property state,
  avoiding a full source/destination snapshot-tree refresh at the end of
  bootstrap and first-replication runs
- progress-enabled send/receive runs now skip size probes entirely when the
  configured `-D` template does not use `%%size%%`, and remote or multi-job
  transfers now prefer cheaper approximate size probes with exact fallback
  instead of always paying an extra `zfs send -nPv` estimate round trip
- adaptive remote source snapshot discovery now keeps send-stream compression
  semantics unchanged but skips `zstd` around `-O ... -j ... -z` metadata
  listing runs, removing compression/decompression overhead from no-op and
  startup-bound discovery paths while preserving stream compression for actual
  replication
- source snapshot list reversal now uses a bounded POSIX-`awk` fast path with
  automatic sort fallback for larger inputs, and that reversal is now also
  deferred until a later consumer actually requests newest-first per-dataset
  source records, avoiding the old unconditional `cat -n | sort -nr | cut`
  path and the newer eager reverse-on-discovery cost on typical no-op runs
  without making large trees depend on unbounded awk memory or weakening
  cross-shell failure handling on the fallback path

The larger discovery, caching, delete, property-path, progress-planning, lazy
snapshot-identity, and remote-startup optimizations have already landed. There
are no queued high-value optimization items at the moment; future work should
come from fresh profiling of real no-op, bootstrap, and multi-host runs rather
than from the older startup bottlenecks this branch has already addressed.

## Remaining Opportunity

The April 7, 2026 no-op trace that originally motivated this review has now had
its last concrete open item addressed: remote snapshot-discovery compression is
adaptive instead of unconditional. The older trace also flagged shell-side
remote dataset counting, eager `name,guid` discovery, private per-process
control sockets, and recursive `-d` no-op dataset scans; those are already
landed optimizations and should not be treated as open work.

There are no queued high-confidence optimization items at the moment. Future
work should come from fresh `-V` timings on real no-op, bootstrap, and
multi-host runs rather than from the older startup bottlenecks that are now
closed.

## Measurement Plan

Any future implementation work should be measured before and after. The safest
first step is lightweight call counting around the existing helpers:

- `-V` is now the intended baseline mode for this work: it emits an end-of-run
  profiling summary with these counters without affecting normal output modes
- count calls to `run_source_zfs_cmd()`, `run_destination_zfs_cmd()`, and
  `invoke_ssh_shell_command_for_host()`
- on `-O` / `-T` startup-sensitive runs, separate first-process measurements
  from warm-cache sibling-process measurements so the remote capability
  handshake cache benefit remains visible
- `exists_destination_calls` now tracks only live destination probes, not
  cache hits, so it can be used directly when measuring the destination-
  existence cache effectiveness
- count those calls separately for source inspection, destination inspection,
  property reconciliation, and send/receive setup
- record wall-clock time for:
  - many datasets / few snapshots
  - few datasets / many snapshots
  - property-heavy recursive runs
  - remote runs with non-trivial RTT
- distinguish first-run or cold-cache behavior from repeated hot-cache behavior

Recommended success metrics for future work:

- fewer `zfs get`, `zfs list`, and snapshot-slice passes per replicated
  dataset/tree
- fewer ssh command invocations on `-O` and `-T`
- better steady-state throughput with `-j`
- no change to replication semantics, rollback safety, or deletion safety

## Safety Notes

Because `zxfer` operates on real ZFS pools and remote hosts, performance work
must keep the existing fail-closed behavior. In practice that means:

- cache only where invalidation is explicit and testable
- keep live probes on correctness-critical branches
- treat destructive paths (`destroy`, `rollback`, full-seed refusal decisions)
  as safety-first even if a shortcut looks faster
- validate every behavior change with unit tests and manual integration runs by
  a human operator
