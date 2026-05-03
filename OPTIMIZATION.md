# Optimization Review

This file tracks performance opportunities for the current branch after a
static comparison with `upstream-compat-final`. That upstream branch is faster
because it does less work overall: remote setup is simpler, temp-file handling
is lighter, background jobs are direct, and it does not perform the same live
destination rechecks or cache integrity checks. Snapshot discovery remains
identity-aware with `name,guid` records, including the fast no-op proof, because
name-only comparison can incorrectly treat same-name snapshots with different
GUIDs as clean.

The optimization goal is to recover throughput and no-op speed without
reintroducing the older safety and injection risks. Items below are candidates,
not approvals to weaken replication correctness, remote quoting, structured
error reporting, secure `PATH`, or cleanup behavior.

No direct host integration or perf harness was run during this review. The
local perf harness touches file-backed ZFS pools through the integration
scaffolding, so automated validation should use the VM matrix boundary when a
disposable guest is available. Direct host runs remain human-only.

## Baseline Differences

- `upstream-compat-final` lists snapshots by `name` for initial source and
  destination inventory. The current branch keeps `name,guid` records in initial
  discovery and the fast no-op proof so same-name GUID divergence is detected
  before zxfer can declare a clean no-op.
- `upstream-compat-final` performs fewer destination probes. The current branch
  rechecks live destination state before receive planning and full-send seeding
  so concurrent destination changes fail closed.
- `upstream-compat-final` uses simple temp files and direct background jobs. The
  current branch uses runtime artifact tracking, atomic cache objects,
  supervised background jobs, and structured cleanup.
- `upstream-compat-final` performs less remote host setup. The current branch
  resolves secure helper paths, validates managed SSH options, manages control
  sockets, and probes remote capabilities.
- `upstream-compat-final` has lighter property reconciliation scaffolding. The
  current branch normalizes machine and human property values, caches per
  dataset, and preserves newline-bearing property values.

## Recently Applied Optimizations

- Initial recursive source and destination snapshot discovery now uses
  `zfs list -o name,guid` consistently. This preserves safe same-name divergence
  detection while keeping the fast no-op proof to one recursive source stream
  and one normalized destination stream.
- Recursive snapshot delta planning now uses one file-backed `comm -3` pass to
  split source-missing and destination-extra records. This removes two large
  shell command substitutions and one full sorted-list scan from no-op planning.
- Remote operating-system detection now warms the full active host capability
  scope, so `-O -j -z` startup does not probe once for `zfs` and then probe
  again for the immediately required `parallel` and compression helpers.
- The launcher now preloads remote helper capabilities before runtime helper
  resolution, but SSH control-socket masters are deferred until snapshot
  discovery proves there is replication work. Clean recursive no-op runs no
  longer pay for opening, checking, and closing an SSH master that would only
  carry a single source snapshot-list command.
- Remote helper capability cache files now live in a stable per-user temp-root
  directory instead of a run-unique directory. Concurrent zxfer processes with
  the same host, secure PATH, SSH policy, and requested helper set can reuse the
  same short-lived capability handshake instead of each probing `uname`,
  `zfs`, `parallel`, and compression helpers separately.
- Remote source snapshot-list metadata now uses the same configured compressor
  as send/receive streams instead of silently strengthening the default from
  `zstd -3` to `zstd -9`. This avoids an unmeasured CPU-heavy metadata path on
  clean no-op runs while still honoring operator-supplied `-Z` choices.
- Destination snapshot normalization now streams prefix rewriting directly into
  `sort` while preserving separate awk and sort failure detection, avoiding one
  full intermediate destination snapshot-list write on large no-op trees.
- Local destination snapshot discovery now stays on unsorted `zfs list -o name`
  output and uses zxfer's streamed prefix-rewrite plus external `sort`. This
  matches upstream's lower-cost local ZFS query shape and avoids pushing a
  potentially expensive name sort into local ZFS during highly concurrent
  no-op runs. The remote-target batch keeps the same canonical local sort after
  its one-round-trip transport so diff ordering stays identical.
- Source snapshot discovery now creates a sorted sidecar inside the existing
  background source-list job. The raw creation-order file remains available for
  later send ordering, but recursive diff planning no longer waits to start the
  source sort only after destination discovery has completed.
- Recursive delta planning now checks for exact sorted source/destination
  equality with `cmp -s` before running the heavier split-diff path. Exact
  no-op runs skip the `comm -3` splitter and its temporary combined-delta file.
- Exclude patterns are now applied to sorted snapshot records before recursive
  delta comparison. Runs such as `-x replica` can now still hit the exact no-op
  `cmp -s` shortcut when the only source/destination differences are excluded
  datasets, instead of paying for full diff splitting and later filtering.
- The source snapshot-list background job now streams through `tee | sort` so
  the raw creation-order capture and sorted sidecar are produced in one pass.
  That removes the previous second read of the full source snapshot file after
  source listing completed, while preserving source, tee, and sort exit-status
  checks.
- Recursive replication now short-circuits clean no-op copy orchestration before
  building the per-dataset iteration list, allocating post-seed staging files,
  refreshing property prefetch state, or preparing deferred SSH control sockets.
- Recursive source no-op discovery now has an identity-aware proof path for `-O`
  with a local destination and no property/migration work. Serial proof uses
  one recursive `name,guid` ZFS query even when `-j` is configured. The proof
  intentionally avoids source-side GNU `parallel` fanout because highly
  concurrent wrapper scripts can multiply per-dataset `zfs list` startup cost
  before zxfer knows there is work to transfer. Changed-source fallback still
  honors `-j` for the full creation-order discovery path.
- Fast recursive no-op startup now defers remote `parallel` capability probing
  when the no-op proof is eligible. The active host preload still warms `zfs`
  and compression helpers, while parallel is resolved lazily only if the proof
  misses and full source discovery actually needs fanout.
- Fast recursive destination no-op discovery keeps the cheap unsorted
  destination snapshot query and streams prefix/exclude normalization directly
  into the same `LC_ALL=C sort` order used by the source proof. A trial
  `zfs list -s name` shape was rejected because ZFS name ordering is not a
  byte-for-byte substitute for the existing `cmp`/`comm` order and can force
  clean no-op runs to fall back into full discovery.
- The fast recursive no-op proof now compares source and destination sorted
  snapshot-name streams through private FIFOs. Exact no-op runs no longer write
  both final sorted lists to local temp files and then read them again for
  `cmp`; mismatch and compare-failure paths terminate both producers and fall
  back to full discovery or fail closed as before.

## Priority Roadmap

This ranking is based on the largest branch differences identified so far and
the likely number of avoided `ssh`, `zfs`, temp/cache, and command-rendering
operations. It is not yet a measured branch-to-branch result. Use
`tests/run_perf_compare.sh` or the VM `perf-compare` layer to confirm the
ordering before making broad implementation changes.

The detailed sections below keep stable numbers for cross-references. The
implementation order should follow this roadmap, not the detail-section number.
The source/target concurrency opportunities below refine the same priorities
with `C` labels.

| Priority | Detailed Items | Expected Gain | Why This Ranks Here |
| --- | --- | --- | --- |
| P0 | Measurement framework: 20 | Enables ranking, not runtime speed | This is already implemented and should be used first. Without same-guest current-vs-`upstream-compat-final` deltas, the rest of this list remains an informed estimate. |
| P1 | Remote discovery and collector work: 1, 2, 11, 12, C1, C2, C6 | Largest expected remote-run gain | The biggest static branch difference is repeated remote setup, remote helper probing, destination discovery, and SSH command fanout. Collapsing these round trips and overlapping independent origin/target reads should dominate remote no-op and startup-bound runs. |
| P2 | Live destination recheck batching and dataset scheduler work: 3, C4, C5 | Largest expected no-op/many-dataset safety-path gain | Live rechecks protect correctness, but repeated destination existence and snapshot probes can dominate no-op recursive runs. Batching or generation-gating keeps the safety check while reducing repeated `zfs list` work, and a dependency-aware scheduler can keep independent target subtrees moving. |
| P3 | Snapshot/property cache shape: 4, 5, 6, 14, C3 | Largest expected large-tree local and remote gain | Per-dataset snapshot/property cache objects, broad property reads, and repeated list passes scale with dataset count. Table-oriented generations plus safe source/destination property prefetch overlap should reduce temp files, cache readbacks, `zfs get` payload, and sort/diff work. |
| P4 | Runtime artifact and cleanup overhead: 9, 24 | High expected gain on temp-heavy and no-op paths | The current branch pays robust artifact tracking and cleanup costs that upstream largely avoids. Bulk operation-scoped temp directories and cleanup-family flags can remove work without weakening cleanup semantics. |
| P5 | Command rendering and execution split: 13, 17, 18, 23, 27 | Medium-to-high startup and remote gain | Repeated quoting, shell command rendering, function probes, and render-before-validation work are broad overheads. They are unlikely to beat remote round-trip reductions, but they affect many paths. |
| P6 | Startup/package shape: 16, 21, 22, 26, 28 | Medium local no-op and usage-path gain | Generated artifacts, lazy dependency resolution, disabled profiling fast paths, and minimal help/usage paths reduce startup costs. These are valuable after larger `zfs`/`ssh` and cache-shape wins are measured. |
| P7 | Background job overhead: 10 | Situational gain on many-send workloads | Supervision overhead matters when many send/receive jobs are launched, but it is less likely to explain remote no-op slowness than discovery and recheck costs. |
| P8 | Encoding, lock identity, backup metadata: 8, 19, 25 | Situational gain on cache-heavy, lock-heavy, backup-heavy runs | These remove repeated helper subprocesses and metadata rendering, but they mainly help specialized workloads. Keep them behind the broader cache and remote-startup work. |
| P9 | Metadata compression threshold and smaller candidates: 7, 15, other candidates | Narrow or workload-dependent gain | These can help specific property-heavy or large-remote-metadata cases, but they should wait for counters showing subprocess loops or compression startup as dominant costs. |

## Source And Target Concurrency Opportunities

The current branch already has some useful concurrency: `zxfer_get_zfs_list`
starts source snapshot listing before destination discovery continues, and
`zxfer_zfs_send_receive` can keep multiple send/receive pipelines active with
`-j` while blocking parent/child destination conflicts. Additional concurrency
should first target independent read-only source and target work. Destination
mutations still need dependency-aware ordering and cache invalidation.

### C1. Prewarm Origin And Target Remote State In Parallel

Current opportunity:

- `src/zxfer_remote_hosts.sh`
- `zxfer_prepare_remote_host_connections`
- SSH control-socket setup
- remote capability preloads

`zxfer_prepare_remote_host_connections` prepares the origin host first and the
target host second. When `-O` and `-T` are both set and refer to different
remote contexts, their control-socket setup and remote capability preloads are
independent. Run those role preparations concurrently, then publish validated
role state back into the main shell.

This needs a small role-state handoff because POSIX subshells cannot mutate the
parent shell's globals directly. Each role worker should write checked state
files for the socket path, lease file, capability payload, resolved helper
paths, stderr, and exit status. The parent should validate the files, preserve
the first failure with structured reporting, and clean up any role that
partially opened a socket.

Safety requirements:

- Only parallelize when origin and target remote contexts are distinct.
- Keep SSH control-socket locks and leases per host/role.
- Preserve wrapper-style host specs, managed SSH option validation, and secure
  remote helper resolution.
- Do not publish partially initialized global state from a failed role.

Measure:

- `g_zxfer_profile_ssh_setup_ms`.
- Origin versus target setup wall time.
- SSH control-socket lock wait time.
- Remote capability live/cache bootstrap source by role.

### C2. Widen Read-Only Source And Target Discovery Overlap

Current opportunity:

- `src/zxfer_snapshot_discovery.sh`
- `zxfer_get_zfs_list`
- `zxfer_write_source_snapshot_list_to_file`
- destination dataset and snapshot inventory helpers

Source snapshot listing already runs while destination discovery proceeds, but
the destination dataset inventory, destination snapshot inventory, source list
post-processing, and cache/index publication still contain serial joins. Treat
the read-only source and target discovery steps as supervised jobs with explicit
outputs, then join once all inputs needed for diffing and cache publication are
ready.

This should not mean restoring broad destination-side per-dataset parallel
snapshot listing by default. The current code notes that the older destination
parallel listing path was not a net win once metadata was cached. Prefer
overlap between independent source and target phases, or a collector that
reduces remote round trips, before adding more target-local `zfs list` fanout.

Safety requirements:

- Keep the destination-missing versus destination-listing-failed distinction.
- Keep source and destination stderr/status separated until the parent merges
  failures into structured reporting.
- Publish snapshot indexes only after all contributing files validate.
- Keep deterministic diff and creation-order semantics.

Measure:

- Source discovery wall time, destination discovery wall time, and join wait
  time.
- Source/destination discovery overlap milliseconds.
- Destination `zfs list` fanout count.
- Snapshot index publish time after joins.

### C3. Run Source And Destination Property Prefetch Concurrently

Current opportunity:

- `src/zxfer_property_cache.sh`
- `zxfer_refresh_property_tree_prefetch_context`
- `zxfer_maybe_prefetch_recursive_normalized_properties`
- `zxfer_prefetch_recursive_normalized_properties`

Recursive property prefetch is currently lazy and per side. When recursive
property transfer or overrides are active, the source and destination property
tables are read-only until the first destination mutation. Start source and
destination prefetch workers after the property prefetch context is refreshed,
then join before the first per-dataset property reconciliation that needs the
tables.

This is likely most useful when source and target are on different hosts or
different pools. On a single host and pool, concurrent `zfs get -r` commands can
increase contention, so same-host concurrency should remain measured and
possibly disabled by default.

Safety requirements:

- Start only before receives, destroys, rollbacks, or property mutations.
- Invalidate or discard destination prefetch state after a receive, rollback,
  destroy, or property-changing operation.
- Preserve machine and human property value handling, including newline-bearing
  property values.
- Treat failed prefetch as failed or unsupported, not as an empty property set.

Measure:

- Source and destination property prefetch wall time.
- Prefetch overlap milliseconds.
- `zfs get` payload bytes and invocation counts by side.
- Cache write/readback counts by side.

### C4. Precompute Read-Only Delete And Grandfather Inputs Across Datasets

Current opportunity:

- `src/zxfer_snapshot_reconcile.sh`
- `zxfer_inspect_delete_snap`
- `zxfer_delete_snaps`
- `zxfer_prefetch_delete_snapshot_creation_times`
- `src/zxfer_replication.sh`
- `zxfer_perform_grandfather_protection_checks`

Delete planning already batches creation-time probes within one dataset, but
the dataset loop still discovers source records, destination records, delete
candidates, and grandfather inputs serially. Build a read-only delete preflight
for all candidate datasets before destructive actions begin, then batch
destination creation-time probes across the full candidate set.

The destructive `zfs destroy` operations should remain ordered at first. Only
the read-only inputs are obvious concurrency candidates. Later, destroys across
unrelated destination subtrees could be considered behind the same ancestry
conflict rules used for send/receive jobs, but that needs stronger failure and
rollback semantics before it is safe.

Safety requirements:

- Keep grandfather protection fail-closed.
- Revalidate or discard preflight data after any destination mutation.
- Keep destructive rollback/destroy ordering conservative until measured and
  covered by tests.
- Preserve the current behavior for `-d`, `-g`, `-F`, and `-Y` interactions.

Measure:

- Delete preflight wall time.
- Destination creation-time `zfs get` batches and payload size.
- Grandfather probe counts.
- Time spent before the first actual destroy.

### C5. Add A Dependency-Aware Dataset Work Scheduler

Current opportunity:

- `src/zxfer_replication.sh`
- `zxfer_copy_filesystems`
- `zxfer_build_replication_iteration_list`
- `zxfer_process_source_dataset`
- `src/zxfer_send_receive.sh`
- destination ancestry conflict checks

The main dataset loop is serial, while send/receive jobs can run in the
background after a dataset reaches the transfer step. A scheduler could split
planning and mutation into bounded work items: read-only inspection,
destination mutation, receive, post-seed property reconcile, and backup
metadata flush. Independent destination subtrees could then advance while a
different subtree is waiting on a long transfer.

This is a larger refactor than simply raising `-j`. The current breadth-first
iteration and destination ancestry conflict checks are correctness boundaries.
A scheduler should keep parent-before-child receive semantics, serialize
mutations that share destination ancestry, and make cache invalidation explicit
per destination generation.

Safety requirements:

- Preserve parent-before-child creation and receive behavior.
- Keep all destination mutations serialized by exact dataset or ancestry
  conflict.
- Make property, snapshot, destination-existence, and backup-metadata cache
  invalidation generation-aware.
- Preserve deterministic failure reporting when multiple workers are active.

Measure:

- Dataset work queue wait time by reason.
- Active send/receive jobs versus configured `-j`.
- Destination ancestry wait time.
- Time between discovery completion and first send, and total elapsed time for
  many independent sibling datasets.

### C6. Let A Remote Collector Use Bounded Internal Fanout

Current opportunity:

- detailed item 2
- `src/zxfer_remote_hosts.sh`
- `src/zxfer_snapshot_discovery.sh`
- `src/zxfer_property_cache.sh`

An ephemeral remote collector can reduce SSH round trips. It can also run
independent read-only metadata commands concurrently inside the remote shell,
for example source snapshot discovery and property prefetch on an origin host,
or destination dataset inventory and selected property reads on a target host.
The collector should return one structured payload only after all child
commands have exited and their statuses have been validated.

This fanout should be bounded and disabled when measurements show it hurts
single-pool local contention. The main benefit is likely remote latency hiding,
not raw ZFS metadata throughput.

Safety requirements:

- Keep per-child stdout, stderr, and exit status distinguishable.
- Fail closed on any missing section, malformed marker, non-zero child status,
  or truncated payload.
- Keep secure `PATH` and wrapper command behavior identical to non-collector
  remote commands.
- Remove staged collector files and remote temp directories on success and
  failure.

Measure:

- Collector child command count and max concurrency.
- Remote collector elapsed time versus sum of child elapsed times.
- Payload bytes and parse time.
- Remote cleanup success/failure counts.

## Detailed Recommendations

### 1. Collapse Destination Discovery Round Trips

Current hot path:

- `src/zxfer_snapshot_discovery.sh`
- `zxfer_get_zfs_list`
- `zxfer_get_destination_dataset_inventory`
- destination snapshot listing helpers

Opportunity:

Destination discovery currently performs separate dataset inventory and snapshot
inventory operations, and remote targets can pay multiple SSH setup and
`zfs list` costs. A target-side helper can emit dataset inventory and snapshot
records in one remote shell invocation using clear section markers. Another
lower-risk step is to widen the existing source/destination overlap by starting
independent read-only destination metadata work as soon as its prerequisites are
known; see C2 for the guardrails around destination-side fanout.

Safety requirements:

- Preserve the distinction between "destination dataset missing" and operational
  listing failures.
- Preserve wrapper-style host specs such as `user@host pfexec`.
- Preserve secure remote helper path resolution.
- Keep stderr and exit-code reporting compatible with structured failure output.

Measure:

- Number of destination SSH invocations.
- Number of destination `zfs list` invocations.
- Destination inventory wall time.
- Total no-op wall time on remote targets.

### 2. Use An Ephemeral Per-Run Remote Collector

Current hot path:

- `src/zxfer_remote_hosts.sh`
- remote capability probe helpers
- `src/zxfer_snapshot_discovery.sh`
- source and destination discovery helpers
- `src/zxfer_property_cache.sh`
- recursive property prefetch helpers

Opportunity:

There is merit in sending or staging a small POSIX `sh` collector on the remote
host for the duration of one zxfer run. The collector can gather remote OS,
helper paths, dataset inventory, snapshot inventory, and selected property
tables in one structured response or through a small set of role-specific
responses. This can reduce SSH startup cost, repeated secure-path setup, command
rendering, and remote `zfs` process fanout without requiring a permanent remote
install.

This should be an ephemeral helper first: `ssh host sh -s`, or a secure per-run
remote temp dir with hash/owner/mode checks if the script is large enough that
staging and reusing it during the same run is cheaper than resending it. A
persistent remote agent or persistent ZFS-state cache is not a near-term
optimization target because external ZFS mutations make safe invalidation hard.

Safety requirements:

- Preserve the no-remote-install default workflow.
- Keep wrapper-style host specs such as `user@host pfexec` and `doas` intact.
- Resolve required remote helpers through the existing secure-path and
  capability rules.
- Use explicit structured section markers and fail closed on malformed,
  truncated, or partially successful collector output.
- If a script is staged, create it under a secure per-run remote temp dir,
  validate owner/mode/hash, and remove it on success and failure.
- Do not persist dataset, snapshot, or property state across zxfer invocations
  without a separate invalidation design.

Measure:

- Remote SSH invocations before first send.
- Remote `zfs list` and `zfs get` invocations.
- Remote startup and discovery wall time.
- Collector payload bytes versus repeated command payload bytes.
- Cleanup reliability for staged remote helper files.

### 3. Batch Or Gate Live Destination Rechecks

Current hot path:

- `src/zxfer_replication.sh`
- `zxfer_reconcile_live_destination_snapshot_state`
- `zxfer_seed_destination_for_snapshot_transfer`
- `zxfer_get_live_destination_snapshots`
- `src/zxfer_exec.sh`
- `zxfer_exists_destination`

Opportunity:

The current branch performs live destination existence and snapshot checks before
receive planning so a destination that changed after initial discovery is caught.
This is important, but doing it per dataset can dominate no-op or many-dataset
incremental runs. Track a destination mutation generation during the run and
reuse a validated live view until the run itself changes the destination, or
batch live snapshot listing for all affected destination datasets.

Safety requirements:

- Keep live rechecks before destructive actions, full-send seeding, rollback,
  deletion, and resume-token decisions.
- Invalidate cached live state after any local receive, rollback, destroy, or
  property-changing operation that can affect later planning.
- Fail closed when a batched live view is incomplete or stale.
- Keep conservative behavior for `-Y`, delete modes, and unknown destination
  state.

Measure:

- `zxfer_exists_destination` calls.
- Live destination `zfs list` calls.
- Per-dataset planning time.
- No-op recursive run time.

### 4. Replace Per-Dataset Snapshot Index Files With A Single Validated Index

Current hot path:

- `src/zxfer_snapshot_state.sh`
- `zxfer_build_snapshot_record_index_core`
- `zxfer_validate_snapshot_record_index_manifest_file`
- `zxfer_get_indexed_snapshot_records_for_dataset`

Opportunity:

The snapshot-record index currently fans out one cache object per dataset, then
validates the manifest and each object. Lookup can also revalidate the whole
index. This creates many temp files, many atomic cache writes, and repeated
readback validation.

Use one validated index file per snapshot-list generation, with dataset sections
or offsets. Validate the index once when it is published, then serve lookups from
that generation without revalidating every object. If lookup speed needs help,
build a small in-memory dataset-to-offset map.

Safety requirements:

- Keep atomic publish semantics for the completed index.
- Keep validation before the index is made visible to planning code.
- Fall back to rebuilding or fail closed on malformed records.
- Preserve newline-safe serialized record handling.

Measure:

- Temp files created during discovery.
- Cache object writes and readbacks.
- Snapshot index build time.
- Snapshot lookup time per dataset.

### 5. Make Property Prefetch Table-Oriented

Current hot path:

- `src/zxfer_property_cache.sh`
- `zxfer_prefetch_recursive_normalized_properties`
- `zxfer_load_normalized_dataset_properties`
- `zxfer_property_cache_store`
- `zxfer_property_cache_dataset_path`

Opportunity:

Recursive property prefetch reads machine and human values for the full tree,
groups them, then stores one cache object per dataset. This preserves tricky
property values but creates many files and repeated key-encoding work.

Keep one grouped property table per side and generation, then look up dataset
records from that table. A compact manifest plus a dataset offset map should be
enough for fast lookups without one cache file per dataset. If per-dataset cache
objects are retained, batch writes and validate once per generation.

Safety requirements:

- Preserve machine-value semantics for commands.
- Preserve human-value expansion for `$inherit`, `$received`, and `-x`.
- Preserve newline-bearing property values.
- Keep cache invalidation tied to the dataset/property generation.

Measure:

- Property prefetch wall time.
- Cache files written.
- Cache readbacks.
- Property reconciliation time.

### 6. Scope Recursive Property Reads To The Required Property Set

Current hot path:

- `src/zxfer_property_cache.sh`
- `zxfer_load_normalized_dataset_properties`
- `zxfer_prefetch_recursive_normalized_properties`

Opportunity:

The current branch often uses `zfs get ... all` for both machine and human
values. For many modes, zxfer only needs a smaller property set: creation-time
filesystem and volume properties, explicitly transferred properties, excluded
properties, and properties required by restore or backup modes. Fetching all
properties can be expensive on large trees and remote targets.

Build the minimum safe property list from the active options, then fetch `all`
only when the selected mode truly needs it. This is especially useful when users
specify a narrow property list with `-o` or when property transfer is disabled.

Safety requirements:

- Keep full property discovery for modes that need unsupported-property
  filtering, backup metadata, restore behavior, or complete property sync.
- Keep creation-time property handling intact for dataset creation.
- Keep inherited/received/local source classification exact.
- Fall back to `all` if the minimum set cannot be proven complete.

Measure:

- `zfs get` payload size.
- Property command wall time.
- Remote property transfer bytes.
- Property-heavy dataset creation time.

### 7. Replace Remaining Shell Property Loops With Batched `awk`

Current hot path:

- `src/zxfer_property_cache.sh`
- `zxfer_resolve_human_vars`
- `zxfer_decode_serialized_property_assignment`
- `src/zxfer_property_reconcile.sh`
- `zxfer_remove_sources`
- `zxfer_select_mc`
- `zxfer_remove_properties`
- property create and set command assembly

Opportunity:

Several property reconciliation helpers still loop in shell and call `echo`,
`cut`, or `awk` per property. These are small on one dataset but expensive on
large recursive runs. Convert the remaining filters, joins, and decodes to
single-pass `awk` helpers over serialized property records.

Safety requirements:

- Preserve delimiter and newline escaping rules.
- Preserve exact source-priority behavior.
- Keep command argument boundaries intact.
- Add regression coverage for commas, equals signs, spaces, and embedded
  newlines in property values.

Measure:

- Subprocess counts for `cut`, `awk`, `echo`, `sed`, and `tr`.
- Property reconciliation wall time.
- Dataset creation command assembly time.

### 8. Cache Encoded Keys And Remote Identity Hex

Current hot path:

- `src/zxfer_property_cache.sh`
- `zxfer_property_cache_encode_key`
- `src/zxfer_remote_hosts.sh`
- `zxfer_remote_capability_cache_identity_hex_for_host`

Opportunity:

The current branch uses `od | tr` to encode cache keys and remote identity
strings. These calls happen repeatedly for datasets, properties, host specs, and
remote capability paths. Cache encoded values in memory for the lifetime of the
process, keyed by the original string and relevant scope.

Safety requirements:

- Include all security-relevant scope in remote identity cache keys.
- Do not reuse encoded keys across different remote command scopes unless the
  existing identity logic says they are equivalent.
- Keep filesystem-safe output for cache paths.

Measure:

- `od` and `tr` subprocess counts.
- Remote setup time.
- Property cache path construction time.

### 9. Slim Runtime Artifact Bookkeeping

Current hot path:

- `src/zxfer_runtime.sh`
- `zxfer_register_runtime_artifact`
- `zxfer_unregister_runtime_artifact`
- `zxfer_create_temp_file_group`
- `zxfer_read_runtime_artifact_file`
- `zxfer_write_runtime_cache_file_atomically`

Opportunity:

Runtime artifact tracking scans and rewrites newline-delimited bookkeeping state
for many temp files. Cache object writes also stage, read back, chmod, rename,
and validate frequently. This is robust but can become O(n squared) when a
single operation creates many short-lived files.

Prefer operation-scoped temp directories registered once, with bulk cleanup of
children. Avoid unregistering every short-lived temp file individually when the
parent directory cleanup is sufficient. Where readback validation is not
security- or correctness-critical, validate command exit status and file size or
checksum once at publish time instead of reading every intermediate file.

Safety requirements:

- Preserve cleanup on both success and failure.
- Keep restrictive permissions for sensitive cache data.
- Keep atomic publish for files consumed by later planning code.
- Preserve checked readbacks where trailing-newline fidelity is part of the
  contract.

Measure:

- Runtime artifacts registered per run.
- Cleanup time.
- Cache object write time.
- Temp-file count in snapshot and property phases.

### 10. Optimize Supervised Background Job Overhead

Current hot path:

- `src/zxfer_background_jobs.sh`
- `src/zxfer_background_job_runner.sh`
- `src/zxfer_send_receive.sh`
- `zxfer_zfs_send_receive`

Opportunity:

Supervised jobs add control directories, launch records, completion records, and
process start-token checks. This is appropriate for long-running send/receive
pipelines, but the per-job setup can still be optimized. Reuse per-run control
state, write fewer metadata files on the success path, and avoid process-token
lookups where the runner can report an equivalent identity safely.

Safety requirements:

- Keep reliable cleanup of failed or interrupted send/receive jobs.
- Keep accurate exit status reporting.
- Keep process identity checks strong enough to avoid killing unrelated
  processes.
- Preserve structured failure records for background job failures.

Measure:

- Background job spawn latency.
- Send/receive setup time before first byte.
- Control files written per transfer.
- Cleanup time after interrupted transfers.

### 11. Cache SSH Transport Rendering And Reduce Control-Socket Probes

Current hot path:

- `src/zxfer_exec.sh`
- `zxfer_get_ssh_transport_tokens_for_host`
- `zxfer_build_ssh_shell_command_for_host`
- `zxfer_invoke_ssh_shell_command_for_host`
- `src/zxfer_remote_hosts.sh`
- control socket setup and `ssh -O` helpers

Opportunity:

Managed SSH options, parsed host specs, wrapper commands, and shell invocation
tokens are rebuilt for many remote commands. Control-socket checks also capture
stderr through temp files. Parse and validate host transport state once per role
after option parsing, then reuse rendered tokens. Check or create the control
socket once when acquiring a lease, then re-check on failure or teardown instead
of around every command.

Safety requirements:

- Preserve wrapper-style host specs and quoting.
- Preserve managed SSH option validation.
- Do not reuse transport state if `ZXFER_SSH_OPTIONS` or secure path context
  changes.
- Keep robust stale-socket cleanup.

Measure:

- SSH shell command construction time.
- `ssh -O check` invocations.
- Remote command startup time.
- Temp files created during remote setup.

### 12. Revisit Remote Capability File Cache Strategy

Current hot path:

- `src/zxfer_remote_hosts.sh`
- `zxfer_ensure_remote_host_capabilities`
- `zxfer_capture_remote_probe_output`
- remote capability file lock helpers

Opportunity:

Remote capability probing uses in-memory and file-backed cache paths with a
short TTL. The file cache can cost more than it saves for a single zxfer process,
especially when the cache root is run-scoped and cleaned on exit. Either make
the cache root stable and permission-checked with an operator-configurable TTL,
or skip file-backed caching for single-process runs and rely on in-memory state.

Safety requirements:

- Include host spec, wrapper scope, secure path scope, and helper identity in the
  cache key.
- Keep lock ownership and mode checks for shared cache files.
- Fail closed on malformed cache content.
- Preserve remote capability probing when cache identity does not match.

Measure:

- Remote capability cache hits and misses.
- Remote probe wall time.
- File lock wait time.
- Remote startup time on first and repeated runs.

### 13. Avoid Duplicate Send/Receive Command Rendering

Current hot path:

- `src/zxfer_send_receive.sh`
- `zxfer_zfs_send_receive`
- `src/zxfer_exec.sh`

Opportunity:

The send/receive path builds display commands and execution commands separately,
then wraps both for remote execution. Render the execution token list once and
derive display strings from that token list only when needed for verbose,
dry-run, or error output. This reduces quoting and remote wrapper work in the
normal fast path.

Safety requirements:

- Keep display output equivalent where it is part of operator-facing behavior.
- Keep command argument boundaries derived from structured tokens, not from
  reparsed display strings.
- Preserve dry-run output.

Measure:

- Send/receive command assembly time.
- Remote wrapper rendering calls.
- Normal incremental transfer startup time.

### 14. Combine Snapshot List Passes

Current hot path:

- `src/zxfer_snapshot_discovery.sh`
- `zxfer_set_g_recursive_source_list`
- `zxfer_reverse_file_lines`
- snapshot diff sort helpers

Opportunity:

Current discovery sorts, diffs, reverses, and derives dataset lists through
several temp files and helper calls. Build the sorted source list, reverse-order
source list, and source dataset inventory in fewer passes over the staged source
snapshot file. Avoid line-count prepasses where a streaming reverse or
sort-based fallback can choose the correct strategy without reading the file
twice.

Safety requirements:

- Preserve creation-order semantics expected by replication planning.
- Preserve deterministic sort order for `comm`.
- Preserve dataset name normalization and newline handling.
- Keep debug counters accurate or update them deliberately.

Measure:

- Snapshot diff sort time.
- File passes over source and destination snapshot lists.
- Temp files created in recursive discovery.

### 15. Threshold Remote Metadata Compression

Current hot path:

- `src/zxfer_snapshot_discovery.sh`
- remote source snapshot list helpers
- remote property and capability capture paths

Opportunity:

When stream compression is enabled, metadata paths may also use compression.
For small snapshot or property lists, compressor startup can cost more than the
bytes saved. Add a threshold or mode-specific heuristic for metadata
compression, independent from data-stream compression, so small metadata payloads
stay uncompressed and large remote trees still benefit.

Safety requirements:

- Keep data-stream compression behavior unchanged unless explicitly requested.
- Preserve remote helper availability checks.
- Keep stderr and exit status handling identical for compressed and uncompressed
  metadata paths.

Measure:

- Metadata bytes transferred.
- Compressor process startup time.
- Remote discovery time with small and large trees.

### 16. Add A Generated Single-File Runtime Artifact

Current hot path:

- `zxfer`
- `src/zxfer_modules.sh`
- all sourced `src/` modules

Opportunity:

The current branch sources many modules. This is good for development and tests,
but no-op runs pay parse and source-time cost. A generated release artifact that
concatenates modules in `src/zxfer_modules.sh` order could reduce startup
overhead while keeping the source tree modular. This should be treated as a
packaging optimization, not a reason to remove the flat source layout.

Safety requirements:

- Keep `src/zxfer_modules.sh` as the single source-order authority.
- Keep direct-sourcing tests against modular files.
- Add CI or packaging checks so the generated artifact cannot drift.
- Document installation and release behavior if the shipped entrypoint changes.

Measure:

- Shell startup time before option parsing.
- No-op elapsed time on local datasets.
- Packaged script size and generation time.

### 17. Fast-Path Shell Quoting And Command Rendering

Current hot path:

- `src/zxfer_exec.sh`
- `zxfer_escape_for_single_quotes`
- `zxfer_quote_token_stream`
- `zxfer_build_shell_command_from_argv`
- `zxfer_render_source_zfs_command`
- `zxfer_render_destination_zfs_command`
- `src/zxfer_reporting.sh`
- report command quoting helpers

Opportunity:

Current command rendering preserves argument boundaries by single-quoting every
token and using helper processes for escaping. That is safe, but expensive when
done for every discovery command, remote wrapper, dry-run string, and failure
context. Add a fast path for tokens that contain only shell-safe bytes, cache
quoted renderings for repeated dataset, host, and helper tokens, and defer report
string rendering until verbose, dry-run, or error output actually needs it.

Safety requirements:

- Keep the slow exact quoting path for any token with whitespace, quotes,
  control bytes, shell metacharacters, or non-printing data.
- Do not derive execution commands from display strings.
- Keep wrapper-style host specs tokenized exactly as they are today.
- Keep structured failure reports available on error paths.

Measure:

- Calls to `zxfer_escape_for_single_quotes`.
- Calls to `sed` from command-rendering helpers.
- Command-rendering time in source and destination discovery.
- Send/receive setup time before the first pipeline starts.

### 18. Split Shell-Pipeline Execution From Argv Execution

Current hot path:

- `src/zxfer_exec.sh`
- `zxfer_execute_command`
- `zxfer_execute_background_cmd`
- `src/zxfer_send_receive.sh`
- pipeline launch helpers
- `src/zxfer_snapshot_discovery.sh`
- background snapshot-list launch helpers

Opportunity:

Some command paths must still use a shell because they contain pipelines,
redirection, remote `sh -c`, or generated helper scripts. Other paths are simple
commands that could avoid `eval`, shell parsing, and display-command rendering
by executing argv directly. Local ZFS helpers already do this; extend the split
to more non-pipeline commands and keep shell execution as an explicit pipeline
or remote-script path.

Safety requirements:

- Never parse display strings back into executable argv.
- Keep pipeline execution for send/receive streams and metadata pipelines that
  require shell syntax.
- Keep failure-context command reporting equivalent.
- Preserve dry-run output where it is operator-visible.

Measure:

- `eval` executions per run.
- Foreground and background command startup time.
- Rendered command string allocations.
- No-op local recursive run time.

### 19. Cache Backup Metadata Keys And Remote Backup Preflight

Current hot path:

- `src/zxfer_backup_metadata.sh`
- `zxfer_backup_metadata_file_key`
- `zxfer_backup_metadata_legacy_file_key`
- `zxfer_build_remote_backup_dir_prepare_cmd`
- `zxfer_ensure_remote_backup_dir`
- remote backup metadata write helpers

Opportunity:

Backup and restore modes build lossless identity keys with `od`, `tr`, `awk`,
and fallback `cksum` logic, then construct large remote shell fragments for
directory preparation, symlink checks, ownership checks, dependency checks, and
atomic writes. Cache current-run dataset-pair keys, cache remote backup
directory preflight by host and path, and batch or reuse remote helper fragments
when multiple metadata records target the same backup root.

Safety requirements:

- Keep lossless current-format metadata keys.
- Keep legacy filename fallback for existing backup files.
- Keep symlink, ownership, mode, and dependency checks before remote writes.
- Invalidate preflight cache if the backup root, host spec, secure path, or
  wrapper scope changes.

Measure:

- Backup metadata key-generation subprocess counts.
- Remote backup preflight invocations.
- Metadata write wall time in backup-heavy runs.
- Remote shell fragment render time.

### 20. Preserve Branch-To-Branch Perf Harness Support

Current hot path:

- `tests/run_perf_tests.sh`
- `tests/run_perf_compare.sh`
- `tests/run_vm_matrix.sh --test-layer perf-compare`
- `docs/testing.md`

Opportunity:

The branch-to-branch comparator is now measurement foundation, not a remaining
runtime optimization. Keep it working and use it before prioritizing large
implementation changes. The branch comparison that matters here is current
branch versus `upstream-compat-final`, run with the same perf profile inside the
same disposable guest and written as an annotated delta. This makes optimization
candidates easier to rank and helps prevent new safety work from hiding large
performance regressions.

Safety requirements:

- Keep direct host perf runs manual-only.
- Run automated branch comparisons inside the VM matrix `perf-compare` layer.
- Keep generated perf artifacts outside shipped source files.
- Do not make perf regressions gating until thresholds are stable.

Measure:

- Same-guest current versus `upstream-compat-final` deltas.
- Profile-counter deltas by phase.
- Mock remote SSH invocation deltas.
- Throughput and no-op wall-clock deltas.

### 21. Lazily Resolve Startup Dependencies And Local Platform State

Current hot path:

- `src/zxfer_modules.sh`
- `zxfer_initialize_dependency_defaults`
- `src/zxfer_runtime.sh`
- `zxfer_init_dependency_tool_defaults`
- `zxfer_init_source_execution_context`
- `zxfer_init_destination_execution_context`
- `zxfer_init_local_awk_compatibility`

Opportunity:

Startup resolves and initializes helpers before the active mode is fully known:
`awk`, `zfs`, optional `parallel`, `ps`, compression command renderings, and
local OS state can all be touched early. Local source and destination context
also ask for local OS information separately, and the awk compatibility check can
ask again. Cache local OS once and defer optional helper resolution until the
feature that needs it is selected.

Safety requirements:

- Keep `zfs` and required helpers resolved before live replication actions.
- Keep `parallel` validation before a selected `-j` parallel source-discovery
  path uses it.
- Keep `ps` and cleanup helpers available before supervised background jobs can
  be spawned.
- Keep SunOS/illumos awk compatibility checks before awk programs that depend on
  a compatible implementation.

Measure:

- Startup latency before CLI parsing and before first `zfs list`.
- `command -v` calls during no-op local runs.
- Duplicate local OS detections.
- Optional helper resolutions avoided on modes that do not use them.

### 22. Add A Fast Disabled Path For Profiling And Timestamps

Current hot path:

- `src/zxfer_reporting.sh`
- `zxfer_profile_metrics_enabled`
- `zxfer_profile_record_zfs_call`
- `zxfer_profile_record_ssh_invocation`
- `zxfer_profile_add_elapsed_ms`
- `src/zxfer_runtime.sh`
- startup and trap timestamp initialization

Opportunity:

Profiling is only emitted with `-V`, but many hot helpers still call profiling
functions and several startup/trap paths compute timestamps even when profiling
is disabled. Cache a numeric profiling-enabled flag after CLI parsing, guard
call sites that are hit for every `zfs` or `ssh` invocation, and avoid
millisecond timestamp work unless `-V` is active or an error path needs it.

Safety requirements:

- Preserve exact `-V` output and counters when profiling is enabled.
- Do not remove timestamps used by structured failure reporting.
- Keep cleanup timing available for `-V`.
- Keep the default non-`-V` output unchanged.

Measure:

- Profiling helper calls when `-V` is not set.
- `date` invocations during normal startup and cleanup.
- Local no-op elapsed time with and without `-V`.
- Difference between default and `-V` run overhead.

### 23. Replace Runtime `command -v` Function Probes With Module Flags

Current hot path:

- `src/zxfer_runtime.sh`
- `zxfer_init_globals`
- `zxfer_trap_exit`
- `src/zxfer_modules.sh`

Opportunity:

The launcher sources modules in a fixed order, but startup and cleanup still use
`command -v function_name` checks before calling reset, cleanup, and relaunch
helpers. These checks support direct-sourcing tests and partial module loads,
but they add repeated shell lookup work on every normal run. Set simple
module-loaded flags when modules are sourced, or split the launcher path from
direct-sourcing test paths so normal execution can call known-loaded helpers
directly.

Safety requirements:

- Preserve direct-sourcing tests that intentionally stop at
  `ZXFER_SOURCE_MODULES_THROUGH`.
- Do not introduce source-time side effects beyond declaring module availability.
- Keep cleanup functions optional in partial test harnesses.
- Keep normal launcher failures fail-closed if a required module did not load.

Measure:

- `command -v` function-probe count during startup and cleanup.
- Startup latency before option parsing.
- Cleanup time on runs with no temp artifacts or background jobs.
- Direct-sourcing test compatibility.

### 24. Add A Zero-Work Cleanup Fast Path

Current hot path:

- `src/zxfer_runtime.sh`
- `zxfer_trap_exit`
- `zxfer_cleanup_registered_runtime_artifacts`
- `zxfer_cleanup_remote_host_cache_roots`
- `src/zxfer_background_jobs.sh`
- `src/zxfer_locking.sh`

Opportunity:

The exit trap checks background job cleanup, cleanup PID tracking, SSH control
sockets, owned locks, runtime artifacts, delete temp files, property cache
directories, snapshot index directories, remote cache roots, and service
relaunch state. Most no-op or early-failing runs have none of these resources.
Track coarse "has work" flags when resources are first registered so the exit
trap can skip entire cleanup families when they were never used.

Safety requirements:

- Set the flag before publishing or exposing the resource that needs cleanup.
- Keep cleanup conservative after partial registration failures.
- Keep signal and failure cleanup paths equivalent once a resource exists.
- Preserve structured cleanup failure reporting.

Measure:

- Cleanup phase wall time.
- Cleanup helper calls on no-op local runs.
- Directory scans under `TMPDIR`.
- Exit-trap overhead in early usage-error paths.

### 25. Cache Lock And Process Identity Metadata

Current hot path:

- `src/zxfer_locking.sh`
- owned lock metadata helpers
- `src/zxfer_remote_hosts.sh`
- SSH control socket lock and lease helpers
- remote capability lock helpers

Opportunity:

Lock and lease safety code reads process identity, boot identity, timestamps,
metadata files, and lock directory state through `date`, `/proc`, `cat`, `awk`,
`sed`, `cksum`, and `od` style helpers. That validation is important, but many
values are stable for the current zxfer process. Cache the current process start
token, boot identity, and rendered lock owner metadata once, then reuse them
when creating or validating zxfer-owned locks and leases.

Safety requirements:

- Never reuse cached identity for another PID.
- Keep stale-lock validation against the lock owner recorded on disk.
- Keep metadata file permission and ownership checks.
- Refresh identity if a helper is executed in a subprocess with a different PID.

Measure:

- Process identity probes per run.
- Lock/lease creation time.
- SSH control-socket setup time.
- Remote capability cache lock wait and validation time.

### 26. Defer Compression Command Rendering Until Compression Is Usable

Current hot path:

- `src/zxfer_runtime.sh`
- `zxfer_init_dependency_tool_defaults`
- `src/zxfer_cli.sh`
- `zxfer_refresh_compression_commands`
- `src/zxfer_dependencies.sh`
- `zxfer_resolve_local_cli_command_safe`

Opportunity:

Default compression commands are initialized and rendered even before CLI
validation knows whether compression can be used. `zxfer_refresh_compression_commands`
also tokenizes command strings and can resolve local `zstd` before a later
usage check rejects `-z` or `-Z` without a remote host. Keep raw compression
strings as defaults, validate empty or malformed `-Z` syntax during parsing, and
resolve or quote compressor/decompressor commands only after consistency checks
prove that a remote compressed path will run.

Safety requirements:

- Keep `-Z` syntax errors reported before any live replication action.
- Keep compressor and decompressor helper resolution before a compressed stream
  or compressed metadata path starts.
- Preserve default `zstd -3` and `zstd -d` behavior for valid `-z` runs.
- Preserve remote helper resolution through the secure-path and capability
  cache flow.

Measure:

- Compression command split and quote calls on runs without `-z`.
- Local `zstd` resolution on invalid `-z` or `-Z` usage.
- Startup latency before consistency-check failures.
- Remote compressed transfer startup time after deferral.

### 27. Defer Remote ZFS Command Rendering Until After CLI Validation

Current hot path:

- `src/zxfer_cli.sh`
- `zxfer_read_command_line_switches`
- `src/zxfer_remote_hosts.sh`
- `zxfer_refresh_remote_zfs_commands`
- `src/zxfer_exec.sh`
- remote command rendering helpers

Opportunity:

The parser refreshes rendered remote ZFS command state as soon as `-O` or `-T`
is seen, before all option combinations have been validated. Runs that will
later fail usage validation can still pay host-spec parsing and command
rendering costs. Store the raw host specs during parsing, then render remote ZFS
commands once after consistency checks and execution-context initialization.

Safety requirements:

- Keep host spec validation before any SSH command can run.
- Preserve wrapper-style host specs and the existing operator-facing error text.
- Keep dry-run display output unchanged.
- Do not defer validation past the first point where remote command state is
  consumed.

Measure:

- Remote command rendering calls during invalid CLI invocations.
- Startup latency for usage errors involving `-O`, `-T`, `-m`, `-c`, `-z`, or
  `-Z`.
- Normal remote run startup time.

### 28. Keep Help And Early Usage Paths Minimal

Current hot path:

- `zxfer`
- `zxfer_prescan_help_flag`
- early failure invocation capture helpers
- `src/zxfer_reporting.sh`
- usage rendering helpers

Opportunity:

The launcher already prescans `-h` before sourcing modules, but it initializes
early failure-context state first. In the default redacted mode this is cheap;
with `ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1`, argument escaping can run even
for a pure help request. Move the help prescan ahead of unsafe invocation
capture, and keep usage-only exits on the smallest path that still preserves the
documented output.

Safety requirements:

- Preserve exact `zxfer -h` output and exit status.
- Keep structured failure invocation capture for real startup and usage errors.
- Do not let untrusted `PATH` affect the help path.
- Keep unsafe invocation fields opt-in only.

Measure:

- `zxfer -h` wall time.
- Helper subprocesses used before help exits.
- Usage-error startup time with and without
  `ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1`.

## Other Candidate Opportunities

- Tune serial versus GNU `parallel` source snapshot discovery for changed-source
  fallback. Clean no-op proof now always uses one recursive `name,guid` list and
  defers `parallel`; the heavier creation-order path still honors `-j`. Fanout
  can lose on small remote trees because `parallel` startup and per-dataset
  command setup dominate, so consider a cheap local threshold, a user-visible
  tuning knob, or reuse of already-needed dataset inventory to pick the cheaper
  path after the proof misses.
- Replace the destination existence cache's shell-string rewrite path with a
  generation table or file-backed map. Rewriting a newline string on every
  create or live update scales poorly in runs with many destination datasets.
  The replacement must preserve fail-closed handling for operational `zfs list`
  errors.
- Avoid source snapshot command staging readbacks when the command text is
  already available in memory and no trailing-newline preservation is needed.
  Keep staged files where a background helper needs a stable handoff boundary.
- Fast-path successful remote capability probe capture. The current temp-dir
  stdout/stderr capture is safest for detailed failure reporting, but successful
  probes may be able to use direct command substitution and reserve staged
  stderr for failure paths.
- Reuse per-run temp directories across related discovery and property phases
  instead of allocating many small temp groups. This is a narrower version of
  the runtime artifact optimization and can be validated one phase at a time.
- Consider a profile-only helper subprocess counter for expensive shell
  utilities. Counting `awk`, `sort`, `comm`, `cut`, `sed`, `od`, `tr`, `ssh`,
  `zstd`, and `parallel` calls would make upstream/current deltas easier to
  rank before larger rewrites.

## Measurement Improvements

The current `-V` profiling counters are useful, but several branch-delta costs
are still hard to see. Add counters before optimizing deeply:

- Snapshot records read, sorted, diffed, and reversed.
- Source and destination metadata bytes transferred.
- Local and remote `zfs list` and `zfs get` invocation counts.
- Live destination recheck counts.
- Temp files, temp directories, cache files, and atomic cache writes.
- Runtime artifact register and unregister counts.
- Cache readbacks and validation failures.
- SSH command renders, control-socket checks, and remote capability probes.
- Background job spawn latency and control files written.
- Source/destination discovery overlap time and join wait time.
- Remote origin/target setup overlap time and per-role setup failures.
- Property prefetch overlap time by side and host.
- Dataset scheduler queue wait time by reason, including job limit,
  destination ancestry, missing parent, and cache-generation invalidation.
- Remote collector child command count, max fanout, and failed child status.
- Helper subprocess counts for `awk`, `sed`, `cut`, `grep`, `sort`, `comm`,
  `od`, `tr`, `zstd`, and `ssh`.

Benchmark shape:

- Compare the current branch against `upstream-compat-final` with the same local
  shell, same OpenZFS version, same dataset tree, and same flags.
- Measure no-op recursive runs, one-snapshot incremental runs, many-dataset
  incremental runs, property-heavy dataset creation, and remote source or remote
  destination runs.
- Use `-V` for operator-facing profile output and add machine-readable counters
  only when they do not change default CLI output.
- Automated agents should prefer VM-backed runs when available:

  ```sh
  ./tests/run_vm_matrix.sh --profile smoke --test-layer perf
  ZXFER_VM_PERF_BASELINE_REF=upstream-compat-final ./tests/run_vm_matrix.sh --profile smoke --test-layer perf-compare
  ```

  Direct host execution of the integration or perf harness remains manual-only.

## Already-Useful Current Optimizations To Preserve

- Source snapshot discovery already overlaps with destination discovery.
- Parallel source snapshot listing avoids serial per-dataset traversal when
  `-j` is used.
- Send/receive background jobs already allow independent destination subtrees to
  proceed while blocking parent/child ancestry conflicts.
- Destination existence checks have a cache path.
- Remote capability probing has an in-memory cache.
- SSH control socket reuse avoids repeated full SSH handshakes.
- `-V` gives a starting point for finding dominant phases.

## Safety Notes

- Do not optimize by removing GUID checks from decisions that can overwrite,
  delete, roll back, or choose an incremental base.
- Do not optimize remote execution by collapsing wrapper host specs into a raw
  hostname.
- Do not bypass secure helper path resolution or managed SSH option validation.
- Do not skip structured failure reporting for faster error exits.
- Do not run destination receives, destroys, rollbacks, or property mutations in
  parallel unless exact-dataset and ancestry conflicts are explicitly modeled.
- Do not treat a failed concurrent metadata worker as an empty source,
  destination, or property table.
- Do not leave temp files, FIFOs, queue directories, control sockets, or cache
  files behind on failure unless an explicit debug mode requested it.
- Any optimization that changes flags, defaults, output, error text, replication
  order, retention, packaging, or test entrypoints needs matching tests and docs.
