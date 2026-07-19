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
`tests/budget_policy.tsv` (universal module/function/test complexity ceilings
plus sensitive call-site ratchets). Performance and call-site budgets ratchet
down; the universal ceilings prevent oversized units without rewarding an
unrelated source-total merge.

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
`runtime_artifact_files_created` 14 -> 11. The clean no-op proof uses regular
files below the existing run root, so `runtime_artifact_dirs_created` remains
zero, and renders its source listing command once in the parent shell
(`command_render_calls` 0 -> 1). These current structural budgets are
documented in `tests/perf_budgets.tsv`.

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
- Structural size: the property-cache module and background-job runner module
  were deleted outright. Path security, lock coordination, and runtime
  artifacts are current concern-specific modules; universal per-module,
  per-function, decision, and test-file ceilings prevent catch-all growth,
  while sensitive caller counts ratchet down in `tests/budget_policy.tsv`.

Recursive property-prefetch grouping was measured again during the module
ownership refactor. The two required recursive `zfs get` views are unchanged,
but their two grouping passes plus merge are now one POSIX `awk` pass and the
staging group falls from seven artifacts to five. The checked-in offline gate
uses deterministic 100- and 1,000-dataset fixtures, alternating samples, exact
byte comparison, and peak-RSS measurement when the host `time` supports it.
Seven-sample medians on macOS were:

| AWK | Datasets | Legacy ms/op | One-pass ms/op | Improvement | Legacy peak RSS | One-pass peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `/usr/bin/awk` | 100 | 28.0 | 13.0 | 53.57% | 3,031,040 | 2,998,272 |
| `/usr/bin/awk` | 1,000 | 190.0 | 98.0 | 48.42% | 3,129,344 | 3,112,960 |
| GNU awk 5.4.1 | 100 | 55.5 | 24.0 | 56.76% | 8,224,768 | 8,224,768 |
| GNU awk 5.4.1 | 1,000 | 346.0 | 170.0 | 50.87% | 9,519,104 | 9,388,032 |

Both implementations produced byte-identical output at both sizes. The
candidate therefore cleared the required 10% large-fixture improvement, the
small-fixture no-regression gate, and the no-RSS-regression gate. These are
grouping costs only; they do not claim end-to-end transfer throughput gains.

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
- Phase 8 — no-op proof widening. The fast recursive no-op
  proof now covers local sources as well as `-O` pulls: a clean local
  recursive no-op is proven from two sorted `name,guid` identity listings
  staged in regular run-root files and never pays for the creation-order source
  listing or the destination existence check. All other eligibility gates
  are unchanged (`-R` required, `-T` absent, no `-s`/`-m`/`-P`/`-o`/`-e`/`-k`),
  and divergence or any stream failure still falls back to full discovery or
  fails closed exactly as before. Current path-security, locking, and runtime
  artifact concerns remain separate modules.
- Refactor follow-up — recursive property-prefetch grouping. Machine and human
  property trees are parsed once into the same machine-first/human-only table
  order as the legacy three-`awk` pipeline. Complete one-line records reuse
  AWK's parsed fields, multiline records are reparsed only when extended, and
  filter membership is released as each selected dataset is materialized.
  Malformed views still fail before publication, embedded values retain the
  existing escaping, and both recursive ZFS calls remain separately checked.

Mapping from the original review's numbered items: 1 (batched destination
discovery), 3 (live recheck gating), 4 (snapshot index flattening), 5
(table-oriented property state), 8 (encoded keys/identity hex — deleted with
their machinery), 9 (runtime artifact slimming), 10 (background job
overhead), 11 (ssh transport memos + socket probes), 12 (capability cache
strategy), 13 (duplicate command rendering), 14 (combined snapshot list
passes), 17 (fast-path quoting), 18 (indirect-assignment and counter `eval`
removal, leaving only the two hardened rendered-shell execution sites), 22
(disabled-profiling fast path), 23 (removal of internal function-existence
probes), 24 (zero-work cleanup), and 25 (lock identity slimming) are DONE.
Item 7's recursive property-prefetch grouping is also complete; other
property-loop candidates remain separate. Item 20 (the perf harness) is
maintained as measurement foundation.

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
- C6. Broaden the existing transactional target-discovery batch with bounded
  internal fanout for other independent read-only remote metadata. Preserve
  its ordered protocol, private workspace, and all-or-nothing publication.

Other remaining items:

- Broader remote collector (remaining part of original item 2): the target
  dataset/snapshot batch and its transactional local publication are complete.
  A future collector could add selected property tables or combine compatible
  helper/OS discovery without a remote install, but must retain exact framing,
  fail closed on truncation, and never persist remote state across runs.
- Property-read scoping (item 6): build the minimum safe property set from
  active options instead of `zfs get ... all` where the mode provably does
  not need full property discovery; fall back to `all` whenever
  completeness cannot be proven.
- Further batched `awk` work for any remaining per-property shell loops in
  reconciliation (the unfinished part of item 7). Recursive property-prefetch
  grouping is already one measured POSIX `awk` pass; future candidates must
  preserve delimiter/newline escaping and source-priority behavior.
- Metadata compression threshold (item 15): small metadata payloads can pay
  more in compressor startup than they save; keep data-stream compression
  unchanged.
- Generated single-file release artifact (item 16): packaging-only; keep
  `src/zxfer_modules.sh` as the source-order authority and keep modular
  files for tests.
- Remote backup preflight caching (item 19): cache remote backup-directory
  preflight per host/path scope; the local metadata buffer is already
  validate-once.
- Lazy startup dependency resolution (item 21) and deferred
  compression/remote-ZFS command rendering (items 26, 27): resolve optional
  helpers and render remote command state only after consistency checks
  prove the mode needs them.
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
- `tests/run_property_prefetch_benchmark.sh --output-dir DIR` — host-safe,
  offline acceptance gate for the recursive property grouping pipeline. It
  never invokes ZFS or the network; raw timing/RSS rows and the gate result are
  retained below `DIR`. `DIR` must be a new child of an existing directory;
  the benchmark preserves an existing path or concurrent race winner and
  fails instead of replacing it. Relative output paths beginning with `-` are
  rejected.
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
