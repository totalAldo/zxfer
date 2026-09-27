# Optimization Record

This file keeps the current performance picture: measured results, the
candidates that remain, how to measure, and the rules no optimization may
break. The history of each wave (what changed, what it measured, which budget
moved) is in `CHANGELOG.txt`.

The goal is to beat `upstream-compat-final` on matched workloads while
keeping the safety work of this tree. Snapshot discovery stays identity-aware
with `name,guid` records, including the fast no-op proof, because name-only
comparison can treat same-name snapshots with different GUIDs as clean.
Compare the same fixture, options and successful work on both refs;
measurements against an older instrumented build or a different helper
inventory do not count.

Two ratchets pin the results: `tests/perf_budgets.tsv` (helper spawns, exact
ssh connections and zfs calls on the small micro-bench fixture) and
`tests/budget_policy.tsv` (sensitive production call sites such as `eval`,
`mktemp` and `$(date`). Both only go down.

## Current Results

Helper spawns counted by `tests/run_microbench.sh` on the default fixture
(25 children x 4 snapshots), with and without `-V` (2026-09-26, after the
next-phase items W3-W5):

| Scenario | `-V` | Plain | ssh connections |
| --- | ---: | ---: | ---: |
| No-op | 21 | 12 | 0 |
| Incremental dry run | 7 | 4 | 0 |
| Incremental | 129 | 109 | 0 |
| Remote no-op (`-O localhost -T localhost`) | 37 | 15 | 1 |
| Remote incremental | 129 | 103 | 1 |
| `props` (incremental, `-P`, 68 matching properties) | 177 | 163 | 0 |

Each remote run opens one ssh master, shared by both roles. On the small
fixture (8 x 2) the incremental spawns 78 helpers with `-V` and `props` 101.

Wall clock against `upstream-compat-final` on the canned zfs, macOS
(`/bin/sh` is bash 3.2), four snapshots per dataset, median seconds of
alternating warmed runs (`tests/run_perf_ab.sh`; the 25-child rows use
`--latency-ms 0 --reps 7`, the 100-child rows `--reps 5`):

| Children | Scenario | Current | Upstream | Ratio |
| ---: | --- | ---: | ---: | ---: |
| 25 | No-op | 0.092 | 0.151 | 0.61 |
| 25 | Incremental | 0.672 | 1.491 | 0.45 |
| 25 | Remote no-op | 0.222 | 0.222 | 1.00 |
| 25 | Remote incremental | 0.916 | 1.985 | 0.46 |
| 100 | No-op | 0.091 | 0.150 | 0.61 |
| 100 | Incremental | 2.690 | 5.760 | 0.47 |

With the default 80 ms mock handshake every remote row is faster than
upstream (remote no-op 0.75). With `-P` and properties that already match,
current code reads each side once through the recursive
`zfs get -r -t filesystem,volume` prefetch and takes 1.15 s at 25 children
against upstream's 75.6 s of per-dataset loops.

Scale readings from the `--snapshots`, `--shell` and `props` knobs, against
`main` (ratios 0.96-1.02, `src/` unchanged): 100 children x 50 snapshots
no-op 0.10 s and incremental 3.79 s; 200 x 100 incremental 16.2 s; `props`
1.34 s and 6.25 s at 25 and 100 children under `/bin/sh`, 1.29 s and 8.22 s
under `--shell /bin/dash`. The next-phase items W7a and W7b target these.

Next-phase results against `main` so far: W3 (`-T` destination discovery
through the ordinary listings) remote no-op 0.74-0.75 at zero latency and
0.83-0.85 at 80 ms, remote incremental 0.97-0.99; W4 (error log without a
lock) a failing run with `ZXFER_ERROR_LOG` set 386 -> 130 ms under bash 3.2
and 303 -> 88 ms under dash; W5 (one property plan program) `props` 0.92-0.93
under `/bin/sh` and dash.

## Remaining Candidates

Unranked, and none is permission to weaken replication correctness, remote
quoting, structured error reporting, secure `PATH` or cleanup. Measure first.

- Snapshot planning at scale: one sort and awk pass per replication pass
  that splits both record files per dataset, so each dataset's plan and live
  recheck read only its slice (next-phase W7a).
- Property table lookup: read only the needed row instead of scanning the
  table twice per dataset; must help dash without slowing bash 3.2 (W7b).
- Multi-line property values in a recursive read: every dataset listed after
  the first ambiguous record falls back to per-dataset reads (3.9 s instead
  of 1.5 s for one such value in a 25-child `-P -R` run). Restarting the
  trusted run at each dataset's first record would confine the fallback, but
  needs a security review: a fake record could then sit in any earlier
  ambiguous value.
- Parallel remote prewarm (C1): run the `-O` and `-T` capability probes
  concurrently when they name distinct hosts; never publish partial role
  state from a subshell.
- Wider read-only discovery overlap (C2): overlap the dataset and snapshot
  inventories, keeping empty-on-failure outputs.
- Dependency-aware dataset scheduler (C5): bounded work items so independent
  subtrees advance during long transfers, keeping parent-before-child
  receives and serialized mutations that share ancestry. A large refactor.
- Property-read scoping: build the minimum property set from the active
  options instead of `zfs get ... all` where completeness can be proven.
- Remote backup preflight caching per host and path; lazy optional-helper
  resolution and deferred remote command rendering until a mode needs them.
- Remote no-op: about 0.75 of `main` at zero latency since W3, when `main`
  was at parity with upstream; the capability probe is the largest remaining
  remote cost.
- Serial versus GNU `parallel` source discovery for the changed-source
  fallback: fanout can lose on small remote trees. The clean no-op proof
  stays one serial recursive stream even with `-j`.

## Measurement

- `tests/run_microbench.sh [-V] [--forks] [-d N -s S] [SCENARIO ...]` counts
  helper spawns (22 tools), `-V` profile counters and ssh connections
  against the canned zfs, for `noop`, `dryrun_incr`, `incr`, `remote_noop`,
  `remote_incr` and `props`. The work directory sits under `/tmp`, so counts
  do not depend on `TMPDIR`. `tests/test_zxfer_microbench_budgets.sh`
  enforces `tests/perf_budgets.tsv` on the 8 x 2 fixture. `--forks` adds
  advisory bash-xtrace subshell counts that are never budgeted.
  `ZXFER_MOCKBIN_ZXFER_BIN` points the bench at another launcher.
- `tests/run_perf_ab.sh --baseline-ref REF [--sizes 25,100] [--snapshots N]
  [--scenarios LIST] [--shell PATH] [--reps N] [--latency-ms 80]
  [--summary FILE]` is the advisory wall-clock A/B on the canned zfs: no
  ZFS, no root, any commit-ish as the baseline. It exits non-zero only for
  harness or usage errors. Every work item reports it against `main` for
  the scenarios it touches; each ratio must stay at or below 1.05.
- `tests/test_contract_failures.sh` fails every zfs and ssh call of nine
  scenarios in turn and requires zxfer to fail closed. An optimization that
  changes call order or error handling must keep it green.
- `tests/run_perf_tests.sh` measures real file-backed pools, only on a
  disposable ZFS host or through the VM matrix: `--test-layer perf`, or
  `--test-layer perf-compare` with `ZXFER_VM_PERF_BASELINE_REF`, which runs
  `run_perf_tests.sh --baseline-bin` against the archived baseline in the
  same guest. `upstream-compat-final` needs a BSD-userland guest and cannot
  run every property case; see `docs/testing.md`.
- `-V` profile counters are the first ranking signal. Their keys are stable:
  deleted machinery's counters read 0 rather than disappear.

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
