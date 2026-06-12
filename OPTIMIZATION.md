# Optimization Record

This file records the performance program that ran as Phases 0-8 on this
branch, with measured results, and keeps a slim list of genuinely remaining
candidates. It started as a static comparison with `upstream-compat-final`,
which was faster mostly because it did less work overall; the program's goal
was to recover that throughput and no-op speed without reintroducing the older
safety and injection risks. Snapshot discovery remains identity-aware with
`name,guid` records, including the fast no-op proof, because name-only
comparison can incorrectly treat same-name snapshots with different GUIDs as
clean.

Budgets that pin these results live in `tests/perf_budgets.tsv`
(micro-bench helper-spawn and profile-counter budgets) and
`tests/budget_policy.tsv` (anti-rebloat line/function/caller budgets). Both
are ratchet-down-only.

## Measured Results

Micro-bench (`tests/run_microbench.sh`, canned zfs, counted helper spawns;
identical across fixture sizes):

| Scenario | Program start | After Phase 6 | After Phase 8 |
| --- | --- | --- | --- |
| no-op recursive, default CLI | 102 spawns | 7 | 7 |
| no-op recursive, `-V` | 176 | 43 | 30 |
| incremental dry run, default CLI | — | 4 | 4 |
| incremental dry run, `-V` | 38 | 9 | 9 |

Notable structural counters on the `-V` no-op path: `cut` 57 -> 0,
`mktemp` 14 -> 1, `sed` 43 -> 3, `awk` 34 -> 4,
`runtime_artifact_files_created` 14 -> 9. The clean no-op proof path adds one
private FIFO directory (`runtime_artifact_dirs_created` 0 -> 1) and renders
its source listing command once in the parent shell
(`command_render_calls` 0 -> 1); both are documented in
`tests/perf_budgets.tsv`.

Remote and structural results:

- Cold incremental `-O` pull: 12 -> 10 ssh invocations; control-socket
  `-O check` probes around commands: 2 -> 0 (one `-M` master open per run,
  multiplexed, one `-O exit` close at exit). One capability probe round trip
  per host per run.
- A clean `-O` pull no-op opens no ssh control master at all
  (deferred-socket behavior), pinned by black-box tests.
- Background jobs: 21 -> 5 helper spawns per send/receive job
  (supervision-lite: setsid process group + one status file, runner module
  deleted).
- Module size: `src/zxfer_remote_hosts.sh` 3,869 -> 1,964 lines (Phase 7);
  the property-cache module and the background-job runner module were deleted
  outright; `src/zxfer_locking.sh` and `src/zxfer_path_security.sh` merged
  verbatim into `src/zxfer_runtime.sh` (Phase 8). Source TOTAL is enforced
  ratchet-down in `tests/budget_policy.tsv`.

## What Landed (Phases 0-8)

- Phase 0 — measurement and pins. Behavior pins for the externally
  observable planning contract (`tests/test_zxfer_planning_blackbox.sh`),
  the canned-zfs micro-bench (`tests/run_microbench.sh`) with ratchet-only
  budgets, and anti-rebloat line/function/caller budgets. The
  branch-to-branch comparator (`tests/run_perf_compare.sh`, VM
  `perf-compare` layer) stays the ranking tool for future work.
- Phase 1 — hot-path micro-overhead. Profiling captures are gated at call
  sites so non-`-V` runs skip recorder work entirely; quoting and
  permission-string parsing run in pure shell on the fast path; profiling
  recorders always return status 0 (`-V` can never change replication
  outcomes).
- Phase 2 — render-once display commands. Operator-facing command strings
  are rendered once and only when verbose/dry-run/error output actually
  consumes them; execution argv is never derived from display strings.
- Phase 3 — cache flattening. Snapshot record lookups serve from flat
  per-run record files; the per-dataset property cache-object module was
  deleted in favor of in-memory property tables with targeted invalidation;
  destination existence answers use an O(1) prepend-only cache; backup
  metadata uses a validate-once buffer.
- Phase 4 — generation-gated live rechecks. Live destination rechecks are
  served from one batched recursive destination listing per
  destination-mutation generation; each receive/destroy bumps the
  generation, and `-Y` invalidates at pass boundaries. Rechecks still happen
  before every mutating decision; they are just no longer per-dataset
  round trips.
- Phase 5 — supervision-lite jobs. The standalone runner process, control
  directories, launch/completion records, and per-job identity revalidation
  were replaced with one backgrounded job shell per job (setsid when
  available), one in-memory registry row, and one status file; the
  descendant reaper lists the full process table (`ps -A`) so cron-launched
  runs reap correctly.
- Phase 6 — per-run temp root. All run-private temp state lives under one
  0700 root from a single `mktemp -d`; allocators hand out children by
  counter with no per-file registration or readback ceremony, and trap exit
  removes everything with one `rm -rf` after jobs, sockets, and locks are
  torn down. Lock metadata slimmed to owner pid + one memoized `ps` start
  token; error-log lock acquisition treats missing/corrupt metadata as busy
  first and corrupt-reaps only after a recheck round, and concurrent
  error-log loss is strictly better than the old baseline.
- Phase 7 — per-run remote state. Remote capability discovery is one
  fail-closed ssh probe per host per run, parsed into memory (capability
  cache files, TTLs, identity hex, cache locks, and wait loops deleted).
  SSH control sockets are per-run, per-role paths under the private temp
  root (socket locks, leases, identity files, and foreign-socket reaping
  deleted). Rendered ssh transport tokens and parsed host/wrapper splits
  are memoized once per role. `-V` counter keys are unchanged (deleted
  machinery's counters now always read 0).
- Phase 8 — no-op proof widening + module merge. The fast recursive no-op
  proof now covers local sources as well as `-O` pulls: a clean local
  recursive no-op is proven from two sorted `name,guid` identity listings
  through private FIFOs and never pays for the creation-order source
  listing or the destination existence check. All other eligibility gates
  are unchanged (`-R` required, `-T` absent, no `-s`/`-m`/`-P`/`-o`/`-e`/`-k`),
  and divergence or any stream failure still falls back to full discovery
  or fails closed exactly as before. `src/zxfer_path_security.sh` and
  `src/zxfer_locking.sh` merged verbatim into `src/zxfer_runtime.sh`.

Mapping from the original review's numbered items: 1 (batched destination
discovery), 3 (live recheck gating), 4 (snapshot index flattening), 5
(table-oriented property state), 8 (encoded keys/identity hex — deleted with
their machinery), 9 (runtime artifact slimming), 10 (background job
overhead), 11 (ssh transport memos + socket probes), 12 (capability cache
strategy), 13 (duplicate command rendering), 14 (combined snapshot list
passes), 17 (fast-path quoting), 22 (disabled-profiling fast path), 24
(zero-work cleanup), and 25 (lock identity slimming) are DONE. Item 20 (the
perf harness) is maintained as measurement foundation.

## Remaining Candidates

These are unranked ideas that survived the program. None are approvals to
weaken replication correctness, remote quoting, structured error reporting,
secure `PATH`, or cleanup behavior. Measure first
(`tests/run_microbench.sh`, `tests/run_perf_compare.sh`, or the VM
`perf-compare` layer).

Concurrency (the C-series from the original review):

- C1. Prewarm origin and target remote state in parallel when `-O` and `-T`
  name distinct remote contexts. Needs a checked role-state handoff because
  subshells cannot mutate parent globals; never publish partially
  initialized role state.
- C2. Widen read-only source/target discovery overlap. Source listing
  already overlaps destination discovery; the remaining serial joins are
  dataset inventory, snapshot inventory, and index publication. Prefer
  overlap or a collector over more destination-side `zfs list` fanout (the
  old destination parallel listing was not a net win).
- C5. Dependency-aware dataset work scheduler: bounded work items
  (inspection, mutation, receive, post-seed reconcile, metadata flush) so
  independent destination subtrees advance during long transfers. Must keep
  parent-before-child receives, serialize mutations sharing destination
  ancestry, and make cache invalidation generation-aware. This is a large
  refactor, not a `-j` tweak.
- C6. Ephemeral remote collector with bounded internal fanout (see also the
  collector item below): run independent read-only remote metadata commands
  concurrently inside one remote shell and return one structured payload,
  failing closed on any malformed or partial section.

Other remaining items:

- Remote collector (original item 2): stage or stream a small POSIX `sh`
  helper per run (`ssh host sh -s`) that gathers OS, helper paths, dataset
  inventory, snapshot inventory, and selected property tables in one
  structured response. Keep the no-remote-install default; fail closed on
  truncated payloads; never persist remote state across runs.
- Property-read scoping (item 6): build the minimum safe property set from
  active options instead of `zfs get ... all` where the mode provably does
  not need full property discovery; fall back to `all` whenever
  completeness cannot be proven.
- Batched `awk` for remaining per-property shell loops in property
  reconciliation (item 7), preserving delimiter/newline escaping and
  source-priority behavior.
- Metadata compression threshold (item 15): small metadata payloads can pay
  more in compressor startup than they save; keep data-stream compression
  unchanged.
- Generated single-file release artifact (item 16): packaging-only; keep
  `src/zxfer_modules.sh` as the source-order authority and keep modular
  files for tests.
- Argv-exec split for more non-pipeline commands (item 18): fewer `eval`
  paths; keep shell execution for real pipelines and remote `sh -c`.
- Remote backup preflight caching (item 19): cache remote backup-directory
  preflight per host/path scope; the local metadata buffer is already
  validate-once.
- Lazy startup dependency resolution (item 21) and deferred
  compression/remote-ZFS command rendering (items 26, 27): resolve optional
  helpers and render remote command state only after consistency checks
  prove the mode needs them.
- Module-loaded flags instead of `command -v` function probes (item 23),
  preserving `ZXFER_SOURCE_MODULES_THROUGH` partial loads for tests.
- Minimal help/early-usage paths (item 28): keep `zxfer -h` on the smallest
  path that preserves documented output.
- Tune serial versus GNU `parallel` source discovery for the changed-source
  fallback: fanout can lose on small remote trees; consider a threshold or
  knob. The clean no-op proof deliberately stays on one serial recursive
  stream even when `-j` is configured.
- Destination existence cache: the prepend-only cache is O(1) per insert,
  but a generation table could simplify invalidation further; preserve
  fail-closed handling for operational `zfs list` errors.

## Measurement

- `tests/run_microbench.sh [-V] [-d N -s S]` — helper-spawn counts and `-V`
  profile counters against the canned zfs; budgets in
  `tests/perf_budgets.tsv` are enforced by
  `tests/test_zxfer_microbench_budgets.sh`.
- `tests/run_perf_compare.sh` and the VM `perf-compare` layer compare this
  branch against a baseline ref inside the same disposable guest:

  ```sh
  ./tests/run_vm_matrix.sh --profile smoke --test-layer perf
  ZXFER_VM_PERF_BASELINE_REF=upstream-compat-final ./tests/run_vm_matrix.sh --profile smoke --test-layer perf-compare
  ```

  Direct host execution of the integration or perf harness remains
  manual-only.
- `-V` profiling counters are the first-stop ranking signal; counter keys
  are stable (deleted machinery's counters read 0 rather than disappearing).

## Safety Notes

- Do not optimize by removing GUID checks from decisions that can
  overwrite, delete, roll back, or choose an incremental base.
- Do not optimize remote execution by collapsing wrapper host specs into a
  raw hostname.
- Do not bypass secure helper path resolution or managed SSH option
  validation.
- Do not skip structured failure reporting for faster error exits.
- Do not run destination receives, destroys, rollbacks, or property
  mutations in parallel unless exact-dataset and ancestry conflicts are
  explicitly modeled.
- Do not treat a failed concurrent metadata worker as an empty source,
  destination, or property table.
- Do not leave temp files, FIFOs, control sockets, or status files behind
  on failure unless an explicit debug mode requested it.
- Any optimization that changes flags, defaults, output, error text,
  replication order, retention, packaging, or test entrypoints needs
  matching tests and docs.
