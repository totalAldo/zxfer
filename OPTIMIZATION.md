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
(25 children x 4 snapshots), with and without `-V` (2026-09-27, after every
next-phase item):

| Scenario | `-V` | Plain | ssh connections |
| --- | ---: | ---: | ---: |
| No-op | 20 | 11 | 0 |
| Incremental dry run | 6 | 3 | 0 |
| Incremental | 104 | 84 | 0 |
| Remote no-op (`-O localhost -T localhost`) | 32 | 13 | 1 |
| Remote incremental | 99 | 77 | 1 |
| `props` (incremental, `-P`, 68 matching properties) | 127 | 113 | 0 |

Each remote run opens one ssh master, shared by both roles. On the small
fixture (8 x 2) the incremental spawns 70 helpers with `-V` and `props` 76.

Wall clock against `upstream-compat-final` on the canned zfs, macOS
(`/bin/sh` is bash 3.2), four snapshots per dataset, median seconds of five
alternating warmed runs (`tests/run_perf_ab.sh --baseline-ref
upstream-compat-final --reps 5 --latency-ms 0`, 2026-09-28):

| Children | Scenario | Current | Upstream | Ratio |
| ---: | --- | ---: | ---: | ---: |
| 25 | No-op | 0.071 | 0.144 | 0.49 |
| 25 | Incremental | 0.508 | 1.360 | 0.37 |
| 25 | Remote no-op | 0.116 | 0.197 | 0.59 |
| 25 | Remote incremental | 0.726 | 1.889 | 0.38 |
| 100 | No-op | 0.076 | 0.157 | 0.48 |
| 100 | Incremental | 1.803 | 5.139 | 0.35 |
| 100 | Remote no-op | 0.115 | 0.196 | 0.59 |
| 100 | Remote incremental | 2.433 | 6.776 | 0.36 |

With the default 80 ms mock ssh handshake, 25 children: remote no-op 0.259 s
against 0.485 s (0.53), remote incremental 1.095 s against 2.563 s (0.43).
With `-P` and 68 properties per dataset that already match, the current code
reads each side once through the recursive `zfs get -r -t filesystem,volume`
prefetch and takes 0.86 s at 25 children; upstream's per-dataset loops took
more than a minute on the same fixture in earlier runs (75.6 s), so `props`
is not part of the upstream comparison.

Against the `main` this phase started from (`f13aed9`), same method with
`--latency-ms 80` (the default) and `--scenarios
noop,incr,remote_noop,remote_incr,props`, 2026-09-27:

| Children | No-op | Incremental | Remote no-op | Remote incremental | `props` |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 25 | 0.82 | 0.83 | 0.71 | 0.86 | 0.70 |
| 100 | 0.83 | 0.82 | 0.73 | 0.88 | 0.67 |

At scale: 200 children x 100 snapshots, incremental, 4.96 s against 15.06 s
(0.33, `--snapshots 100`); `props` at 100 children under `--shell /bin/dash`,
3.57 s against 7.27 s (0.49); `-e` at 400 children now costs about the same
as `-P` (it added 23-25 s under `/bin/sh`); a failing run with
`ZXFER_ERROR_LOG` set takes 130 ms instead of 386 ms under bash 3.2. The
`props` fixture's canned zfs scans a manifest of about five lines per dataset
on every call, which both trees pay: without it the `-P` work at 200 children
is about 2.2 s in either shell. Which change produced which gain is recorded
per item in `CHANGELOG.txt`.

## Remaining Candidates

Unranked, and none is permission to weaken replication correctness, remote
quoting, structured error reporting, secure `PATH` or cleanup. Measure first.

- `-k` forwarded provenance: with an earlier `-k` hop's alias present, each
  dataset runs one `awk` over every forwarded row
  (`zxfer_resolve_forwarded_backup_metadata`); the property row store could
  serve those rows the way it serves `-e`.
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
- `-T` no-op: `-T` disables the fast no-op proof
  (`zxfer_fast_recursive_noop_options_are_eligible`), so a clean `-T` no-op
  runs full discovery. Lifting the gate would list the destination over the
  target's master inside the proof; measure it (remote no-op, with and
  without mock latency) before relying on it. The capability probe is the
  largest remaining remote cost.
- Serial versus GNU `parallel` source discovery for the changed-source
  fallback: fanout can lose on small remote trees. The clean no-op proof
  stays one serial recursive stream even with `-j`.
- Harness: the canned zfs (`tests/mock_toolchain_helper.sh`) scans its
  manifest line by line on every call, and the `props` fixture adds about
  five rules per dataset, a cost both trees pay, so A/B ratios understate
  `-P` gains in zxfer itself. An indexed manifest would let
  `tests/run_perf_ab.sh` show them.

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
