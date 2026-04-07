# Optimization Review

This document catalogs performance opportunities found by reviewing the current
`zxfer` codebase. It focuses on four areas the project already cares about:

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
- origin snapshot discovery can already parallelize with GNU `parallel`
- concurrent send/receive jobs already exist behind `-j`
- ssh control sockets are reused for `-O` and `-T`

The suggestions below target the places where the remaining overhead is still
noticeable.

## Highest-Value Opportunities

### 1. Batch `zfs set` operations per dataset

Where:

- `apply_property_changes()` in `src/zxfer_transfer_properties.sh`
- `zxfer_run_zfs_set_property()` in `src/zxfer_transfer_properties.sh`

What happens now:

- property updates are applied one property at a time
- for remote targets, each property becomes a separate ssh-backed `zfs set`
  round trip

Why it matters:

- recursive property sync on large trees turns into many small remote calls
- latency dominates on `-T` runs even when the actual property work is trivial

Suggestion:

- batch consecutive property sets for the same dataset into a single
  `zfs set prop=value ... dataset` call after diffing is complete
- keep dry-run output expanded enough to stay operator-readable
- do not batch across dataset boundaries or mix `set` and `inherit` in the same
  execution path
- verify whether repeated `zfs inherit` calls can also be safely collapsed on
  the supported platforms; if not, leave inherit one-at-a-time

Expected impact:

- high for `-P`, `-o`, post-seed property reconciliation, and remote targets

### 2. Cache raw normalized property reads per iteration

Where:

- `get_normalized_dataset_properties()` in `src/zxfer_transfer_properties.sh`
- `collect_source_props()` and `collect_destination_props()`
- `ensure_required_properties_present()`
- `adjust_child_inherit_to_match_parent()`

What happens now:

- each normalized property read performs two `zfs get all` calls per dataset
  (`-Hpo` and `-Ho`)
- `ensure_required_properties_present()` may add extra per-property `zfs get`
  calls for missing creation-time properties
- parent destination properties can be re-fetched repeatedly while processing
  sibling datasets

Why it matters:

- property transfer is one of the most metadata-heavy phases in recursive mode
- remote `-O` and `-T` runs pay ssh latency on every repeated property read
- sibling datasets often share the same parent, so repeated parent reads are
  avoidable

Suggestion:

- add source-side and destination-side property caches scoped to a single
  replication iteration, and clear them at the start of each `-Y` pass
- key caches by inspection side plus dataset, not dataset name alone, so source
  and destination lookups cannot collide
- cache the raw normalized property list returned by ZFS inspection, then apply
  restore-mode overrides, ensure-writable handling, readonly filtering, ignore
  filtering, and unsupported-property filtering per caller
- cache the destination parent property reads used by
  `adjust_child_inherit_to_match_parent()` so shared parents are only fetched
  once per iteration
- invalidate destination-side cache entries after any operation that can change
  effective destination properties on that dataset, including create, receive,
  `zfs set`, and `zfs inherit`

Expected impact:

- high on recursive property-heavy runs, especially over ssh

### 3. Remove the quadratic delete-mapping path

Where:

- `get_dest_snapshots_to_delete_per_dataset()` in
  `src/zxfer_inspect_delete_snap.sh`

What happens now:

- `comm` first finds identities that exist only on the destination
- then each identity is matched back to the full destination snapshot path by
  rescanning the entire destination list

Why it matters:

- this is effectively O(deletes * destination_snapshots) for each dataset
- the cost grows quickly on snapshot-heavy destinations

Suggestion:

- build the destination identity-to-path mapping once and resolve deletions in a
  single pass against that map
- an `awk`-based set/mapping pass is a good fit here because it can preserve
  destination order without rescanning
- keep the final destroy list in destination order so grandfather checks,
  rollback eligibility, and operator-visible output stay aligned with the live
  target state

Expected impact:

- high on deletion-heavy trees or long-retention destinations

### 4. Replace shell string membership tests with set-based matching

Where:

- `get_last_common_snapshot()` in `src/zxfer_inspect_delete_snap.sh`
- `reconcile_live_destination_snapshot_state()` in `src/zxfer_zfs_mode.sh`

What happens now:

- destination snapshot identities are concatenated into one large newline-padded
  shell string
- each source snapshot then checks membership with a shell `case` substring
  match

Why it matters:

- repeated full-string scans scale poorly on large lists
- large concatenated shell strings also increase memory churn

Suggestion:

- replace string membership with a file-backed or `awk`-backed set lookup keyed
  by snapshot identity (`name+guid`)
- keep the source-side scan order so the newest common snapshot is still chosen
  correctly
- apply the same identity-set approach to the live destination recheck path so
  initial planning and late reconciliation use the same matching semantics

Expected impact:

- high on deep histories and large recursive trees

### 5. Replace batch-barrier job control with a rolling worker pool

Where:

- `zfs_send_receive()` in `src/zxfer_zfs_send_receive.sh`
- `wait_for_zfs_send_jobs()` in `src/zxfer_zfs_send_receive.sh`

What happens now:

- once the job limit is reached, zxfer waits for all running jobs to finish
  before starting the next batch

Why it matters:

- one slow transfer stalls the whole next wave
- throughput drops whenever job durations are uneven, which is common across
  mixed-size datasets

Suggestion:

- replace the current batch barrier with a rolling concurrency limiter
- a FIFO/token semaphore or per-job status-file approach would preserve POSIX
  shell portability while keeping the pipeline full
- preserve the current fail-fast behavior: on the first non-zero send/receive
  exit, stop scheduling new work, terminate remaining in-flight jobs, and
  surface the failing PID/status in the final error

Expected impact:

- high when `-j` is used for actual replication, especially with uneven dataset
  sizes

## Medium-Value Opportunities

### 6. Collapse redundant destination existence probes

Where:

- `exists_destination()` in `src/zxfer_common.sh`
- callers in `src/zxfer_get_zfs_list.sh`, `src/zxfer_zfs_mode.sh`, and
  `src/zxfer_transfer_properties.sh`

What happens now:

- many code paths probe destination existence with a fresh `zfs list -H`
  command
- some flows probe first and then immediately run a second command that would
  have answered the same question

Examples:

- destination snapshot discovery checks existence before issuing the snapshot
  list command
- `copy_snapshots()` and `reconcile_live_destination_snapshot_state()` each
  re-check destination existence live
- `ensure_destination_exists()` may check whether the parent exists with a new
  probe even though `g_recursive_dest_list` already exists

Suggestion:

- add a lightweight per-iteration destination existence cache seeded from
  `g_recursive_dest_list`
- update it when zxfer creates a dataset or receives into a new one
- keep live rechecks only on fail-closed paths where stale state would be
  unsafe, such as destructive rollback or full-seed refusal logic

Expected impact:

- medium; largest win is on remote targets with many datasets

### 7. Avoid full-tree refreshes after seed receives when only property state changed

Where:

- `copy_filesystems()` and `refresh_dataset_iteration_state()` in
  `src/zxfer_zfs_mode.sh`

What happens now:

- if any dataset needed post-seed property reconciliation, zxfer refreshes the
  entire source and destination snapshot state before that final property pass

Why it matters:

- this rebuilds lists that the property pass does not actually need
- on large recursive trees the refresh can dominate the tail end of a run

Suggestion:

- track newly created/seeded datasets incrementally and update
  `g_recursive_dest_list` in memory
- only re-read the specific destination property state needed for the post-seed
  pass
- keep the broader refresh for paths that actually change snapshot planning
  inputs, such as new source snapshots or a fresh `-Y` iteration

Expected impact:

- medium on bootstrap or first-replication runs

### 8. Make source snapshot discovery adaptive instead of always N+1 on `-j`

Where:

- `build_source_snapshot_list_cmd()` in `src/zxfer_get_zfs_list.sh`

What happens now:

- serial mode uses one recursive `zfs list`
- parallel mode first lists datasets, then runs one snapshot-list command per
  dataset via GNU `parallel`

Why it matters:

- the current `-j` discovery path can be better on cold caches and very large
  trees
- it can also be worse on hot caches, low dataset counts, or high ssh latency
  because it multiplies command startup overhead

Suggestion:

- add an adaptive threshold so zxfer chooses between single-call and per-dataset
  discovery based on dataset count, local-vs-remote execution, and measured ssh
  startup cost
- if the interface should stay unchanged, start with an internal heuristic and
  expose tuning only if needed later

Expected impact:

- medium; highly workload-dependent

### 9. Replace sort-based reversal with a linear reversal path

Where:

- `reverse_file_lines()` and `reverse_numbered_line_stream()` in
  `src/zxfer_get_zfs_list.sh`

What happens now:

- ascending source snapshot lists are reversed via `cat -n | sort -nr | cut`

Why it matters:

- this is an O(n log n) sort for a task that is logically just reversal
- it also adds extra process and I/O overhead on very large snapshot lists

Suggestion:

- replace the sort pipeline with a POSIX `awk` reversal pass
- if memory usage becomes a concern on huge trees, document the trade-off and
  benchmark both implementations before switching

Expected impact:

- medium on large snapshot inventories, low elsewhere

## Lower-Priority Cleanup

### 10. Reduce extra send probes when the progress bar is enabled

Where:

- `calculate_size_estimate()` and `handle_progress_bar_option()` in
  `src/zxfer_zfs_send_receive.sh`

What happens now:

- each progress-enabled transfer performs an extra `zfs send -nPv` estimate

Why it matters:

- the progress feature doubles the number of send-planning probes for that
  dataset
- on remote origin hosts this also adds latency

Suggestion:

- keep the current behavior for accuracy, but consider a cheaper estimate mode
  for `-j` or remote runs
- another option is to make the expensive estimate optional under a separate
  progress style later

Expected impact:

- low to medium, depending on how often `-D` is used

### 11. Replace small nested shell loops in property diff helpers with one-pass `awk`

Where:

- `validate_override_properties()`
- `derive_override_lists()`
- `diff_properties()`
- `adjust_child_inherit_to_match_parent()`
- `calculate_unsupported_properties()`

What happens now:

- many helpers repeatedly split the same strings with `cut`, `grep`, `tr`, and
  nested shell loops

Why it matters:

- each individual list is small, so these are not the first bottleneck to fix
- however, the code does a lot of process spawning in the property path

Suggestion:

- once the higher-value `zfs`/`ssh` round-trip reductions are done, consider
  moving list diffing and property-field extraction into one-pass `awk`
  transforms
- do this only if profiling still shows the shell-side parsing to be material

Expected impact:

- low by itself; mostly a cleanup after the larger wins above

## Suggested Implementation Order

1. Batch `zfs set` operations.
2. Add per-iteration property caches with explicit invalidation.
3. Remove the O(n^2) destination-delete mapping.
4. Replace common-snapshot substring matching with set-based lookups.
5. Convert `-j` send scheduling from barrier batches to a rolling pool.
6. Collapse redundant destination existence checks.
7. Revisit adaptive source discovery and linear reversal once the larger
   metadata costs are down.

## Measurement Plan

Any implementation work should be measured before and after. The safest first
step is lightweight call counting around the existing helpers:

- `-V` is now the intended baseline mode for this work: it emits an end-of-run
  profiling summary with these counters without affecting normal output modes
- count calls to `run_source_zfs_cmd()`, `run_destination_zfs_cmd()`, and
  `invoke_ssh_shell_command_for_host()`
- count those calls separately for source inspection, destination inspection,
  property reconciliation, and send/receive setup
- record wall-clock time for:
  - many datasets / few snapshots
  - few datasets / many snapshots
  - property-heavy recursive runs
  - remote runs with non-trivial RTT
- distinguish first-run or cold-cache behavior from repeated hot-cache behavior

Recommended success metrics:

- fewer `zfs get` and `zfs list` calls per replicated dataset
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
