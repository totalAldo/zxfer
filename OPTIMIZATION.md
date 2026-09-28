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

The current simplification target is to exceed `upstream-compat-final` in
representative matched workloads while retaining the safety and feature
enhancements from `optimize/performance`. Measurements against an older
instrumented build or a different helper inventory do not establish that
target; compare the same fixture, options, and successful work on both refs.

Budgets that pin these results live in `tests/perf_budgets.tsv`
(micro-bench helper-spawn and profile-counter budgets) and
`tests/budget_policy.tsv` (sensitive call-site ratchets). Performance and
call-site budgets ratchet down; the former universal size ceilings were
removed on 2026-09-01 because they forced module and function splitting.

## Measured Results

### Next phase W1: fail-closed sweep and measurement knobs (2026-09-26)

W1 changed tests and tooling only; `src/` is unchanged, so the micro-bench
counts of the existing scenarios are identical (the new `props` scenario
spawns 203 / 101 helpers with `-V` at 25x4 / 8x2). The fail-closed sweep
(`tests/test_contract_failures.sh`) found no fail-open.

`tests/run_perf_ab.sh --baseline-ref upstream-compat-final --sizes 25
--latency-ms 0 --reps 7` on macOS (`/bin/sh` is bash 3.2), two runs, median
seconds, ratio to upstream:

| Children | Scenario | Current | Upstream | Ratio |
| ---: | --- | ---: | ---: | ---: |
| 25 | No-op | 0.092, 0.091 | 0.151, 0.151 | 0.61, 0.60 |
| 25 | Incremental | 0.672, 0.656 | 1.491, 1.416 | 0.45, 0.46 |
| 25 | Remote no-op | 0.222, 0.204 | 0.222, 0.209 | 1.00, 0.97 |
| 25 | Remote incremental | 0.916, 0.939 | 1.985, 1.991 | 0.46, 0.47 |

The zero-latency remote no-op is at parity with upstream; the wave 4
comparison measured 1.25.

First readings from the new knobs against `main` (same host, median of 3,
ratios 0.96-1.02 because `src/` is unchanged): 100 children x 50 snapshots
(`--snapshots 50`) no-op 0.10 s, incremental 3.79 s; 200 x 100 incremental
16.2 s (one run); `props` (`-P`, 68 matching properties per dataset)
1.34 s / 6.25 s at 25 / 100 children under `/bin/sh` and 1.29 s / 8.22 s
under `--shell /bin/dash`.

### Wave 9 property-read and verbose-escaping fixes (2026-09-26)

Wave 9 changed no path that runs without `-v`, `-V` or `-P`. A user property
whose lone re-read reports the source `-` is now skipped (no extra call), and
the `-v`/`-V` property lines escape values: under `-V` each property transfer
adds up to eight subshells for the list dumps (`awk` runs only for a list
holding a non-printable byte or a backslash), and under `-v` each apply with
work adds one or two.

`tests/run_microbench.sh` counts are identical to wave 8 with and without
`-V` (no-op 21 / 12, incremental dry run 7 / 4, incremental 129 / 109, remote
no-op 47 / 25, remote incremental 135 / 110), so no budget moved.

Local canned-fixture A/B against `upstream-compat-final` (no `-P`, 4
snapshots per dataset, median seconds of five alternating warmed runs, macOS,
`/bin/sh` is bash 3.2):

| Children | Scenario | Current | Upstream | Ratio |
| ---: | --- | ---: | ---: | ---: |
| 25 | No-op | 0.089 | 0.146 | 0.61 |
| 25 | Incremental | 0.657 | 1.419 | 0.46 |
| 100 | No-op | 0.091 | 0.150 | 0.61 |
| 100 | Incremental | 2.690 | 5.760 | 0.47 |

### Wave 7 property-parse fix and CI wiring (2026-09-25)

Wave 7 fixed the record-shaped property value injection (see `CHANGELOG.txt`)
and wired the fuzz and advisory perf CI jobs. Only `-P`/`-o` runs change:
every property read adds one `zfs get` that lists the names alone, and
records after a multi-line value are re-read one property at a time.

`tests/run_microbench.sh` counts are identical to wave 6 with and without
`-V` on both fixtures (no-op 21 / 12, incremental dry run 7 / 4, incremental
129 / 109, remote no-op 47 / 25, remote incremental 135 / 110 at 25x4), so no
budget moved.

`-P -R` incremental, 25 children, about 60 properties per dataset, with
destination properties that already match (canned zfs, macOS, `/bin/sh` is
bash 3.2), against the wave 6 tree:

| Case | Wave 7 | Wave 6 | `zfs get` calls |
| --- | ---: | ---: | --- |
| No multi-line value (median of 5) | 1.470 | 1.481 | 6 vs 4 |
| One local multi-line value on the first child (median of 3) | 3.948 | 1.599 | 160 vs 4 |

With a multi-line value every dataset listed after it falls back to
per-dataset reads (3 gets each), and the dataset holding it re-reads each
later property alone (2 gets each). An inherited multi-line value already made
every dataset fall back. A possible later optimization, not done: restart the
trusted run at each dataset's first record in a recursive read, so only the
affected dataset falls back; its only added exposure would be the
recreated-dataset race recorded in `KNOWN_ISSUES.md`.

Local canned-fixture A/B against `upstream-compat-final` (no `-P`, 4
snapshots per dataset, median seconds of five alternating warmed runs, host
under other load):

| Children | Scenario | Current | Upstream | Ratio |
| ---: | --- | ---: | ---: | ---: |
| 25 | No-op | 0.105 | 0.170 | 0.61 |
| 25 | Incremental | 0.744 | 1.667 | 0.45 |
| 100 | No-op | 0.102 | 0.171 | 0.60 |
| 100 | Incremental | 2.972 | 6.651 | 0.45 |

### Wave 6 ssh-connection gate and advisory A/B (2026-09-25)

Wave 6 changed no hot path. It fixed BSD and illumos background-job teardown
(background job shells run `/bin/sh`; FreeBSD sh, dash and ksh93 use the
cleanup wrapper when `setsid` is unavailable) and added test tooling: the
micro-bench now also runs `remote_noop` and `remote_incr`
(`-O localhost -T localhost` through a socket-aware mock ssh) and pins their
ssh connections exactly, and `tests/run_perf_ab.sh` replaces the scratch A/B
harnesses.

`tests/run_microbench.sh -V` helper totals, 25x4 / 8x2 fixtures, are
unchanged from wave 4: no-op 21 / 21, incremental dry run 7 / 7, incremental
129 / 78; without `-V` the 25x4 runs spawn 12, 4 and 109 helpers. The new
remote scenarios spawn 47 / 47 (no-op) and 135 / 84 (incremental) helpers
with `-V`, and 25 and 110 without it. Every remote run opens exactly one ssh
connection (one master shared by both roles) and makes 7 (no-op) or 60 / 26
(incremental) ssh calls.

`tests/run_perf_ab.sh --baseline-ref upstream-compat-final --reps 3
--sizes 25` on macOS (`/bin/sh` is bash 3.2), 80 ms mock ssh handshake,
`date +%s%N` clock, median seconds of three alternating warmed runs:

| Children | Scenario | Current | Upstream | Ratio |
| ---: | --- | ---: | ---: | ---: |
| 25 | No-op | 0.093 | 0.160 | 0.58 |
| 25 | Incremental | 0.630 | 1.489 | 0.42 |
| 25 | Remote no-op | 0.344 | 0.459 | 0.75 |
| 25 | Remote incremental | 1.267 | 2.734 | 0.46 |

### Wave 4 sweep and current comparison (2026-09-24)

Wave 4 removed dead and single-use helpers and made the temp-file,
helper-lookup, zfs-render, and source-listing helpers publish result globals
instead of printing, so their callers no longer fork a command substitution.
The remote endpoint context, `-z` codec lookups, the origin listing, and the
remote backup program are resolved and rendered in the current shell, and
the short ssh socket directory is chosen once per run.

`tests/run_microbench.sh -V` helper totals, 25x4 / 8x2 fixtures: no-op 22 ->
21 / 22 -> 21, incremental dry run 7 / 7, incremental 131 -> 129 / 80 -> 78
(`cat` 36 -> 34). Without `-V` the 25x4 runs spawn 12 (no-op), 4 (dry run),
and 109 (incremental) helpers. The new advisory `--forks` count (bash 5 in
posix mode) sees 18 subshell forks on the no-op run (11 startup, 4 during
replication, 3 at exit), 14 on the dry run, and 126 on the incremental run,
against 19, 14, and 129 on the wave 3r tree.

`ab.py` alternating A/B against `upstream-compat-final` on macOS (`/bin/sh` is
bash 3.2), canned ZFS, four snapshots per dataset, seven warmed repetitions
(median seconds; zfs calls include the mock's `END receive` lines). The `-P`
row comes from a separate alternating harness on an incremental fixture whose
source and destination properties already match, so no property is changed
(three warmed repetitions). A 100-child `-P` run was stopped after about five
minutes, roughly halfway through upstream's first run, so it has no row:

| Children | Scenario | Current | Upstream | Ratio | ZFS calls, current / upstream |
| ---: | --- | ---: | ---: | ---: | ---: |
| 25 | No-op | 0.087 | 0.141 | 0.62 | 2 / 4 |
| 25 | Incremental | 0.625 | 1.348 | 0.46 | 83 / 108 |
| 100 | No-op | 0.087 | 0.144 | 0.60 | 2 / 4 |
| 100 | Incremental | 2.542 | 5.490 | 0.46 | 308 / 408 |
| 25 | Incremental with `-P` | 1.152 | 75.563 | 0.02 | 86 / 264 |

The `-P` fixture answers both refs' property queries from the same tables:
current code reads each side once through the recursive
`zfs get -r -t filesystem,volume` prefetch (8 list and get calls at 25
children), while upstream reads each dataset (186 calls). Upstream is much
slower on this host than in the September session below (75.6 s against
15.6 s at 25 children; one run on the September fixture also took 71 s), so
compare the two refs within a row, not across sessions.

Remote runs (mock measurements, not a real network), same method as the wave
3r table below: 25 children, median of 3 warmed runs, all three refs
alternating in one session. "Before" is the wave 3r tree (`f17e560`). ssh argv
is byte-identical to that tree in every row. Seconds, with the ratio to
upstream:

| Handshake | Case | Scenario | Before | Current | Upstream |
| ---: | --- | --- | ---: | ---: | ---: |
| 80 ms | plain | No-op | 0.417, 0.90 | 0.395, 0.85 | 0.463 |
| 80 ms | plain | Incremental | 1.358, 0.53 | 1.323, 0.52 | 2.553 |
| 80 ms | doas | No-op | 0.435, 0.92 | 0.422, 0.89 | 0.473 |
| 80 ms | doas | Incremental | 1.521, 0.58 | 1.498, 0.57 | 2.643 |
| 80 ms | `-z` | No-op | 0.444, 0.95 | 0.404, 0.86 | 0.468 |
| 80 ms | `-z` | Incremental | 1.535, 0.57 | 1.483, 0.55 | 2.683 |
| 0 | plain | No-op | 0.262, 1.34 | 0.245, 1.25 | 0.196 |
| 0 | plain | Incremental | 0.936, 0.53 | 0.914, 0.52 | 1.774 |
| 0 | doas | No-op | 0.280, 1.37 | 0.263, 1.29 | 0.204 |
| 0 | doas | Incremental | 1.086, 0.59 | 1.070, 0.58 | 1.844 |
| 0 | `-z` | No-op | 0.292, 1.48 | 0.250, 1.27 | 0.197 |
| 0 | `-z` | Incremental | 1.112, 0.58 | 1.067, 0.56 | 1.910 |

With an 80 ms handshake every remote row is faster than upstream. With no
handshake cost the remote no-op was still slower than upstream in this
comparison; it measured at parity on 2026-09-26 (see the W1 section above).

### Wave 3r remote connection lifecycle (2026-09-23)

Each `-O`/`-T` role's ssh control master now opens before its first remote
command, so the capability probe, the `-O` source listing, and the `-T`
discovery batch multiplex over it instead of each opening a direct
connection. Under `BatchMode=yes` two masters handshake concurrently. The
`-T` discovery batch stages its side files in one `mktemp -d` workspace, and
long remote `sh -c` scripts render in one pass (the 3.6 KB discovery batch in
8.5 ms instead of 32 ms under bash 3.2).

Latency harness (mock measurements, not a real network): a mock ssh charges a
fixed handshake for every new connection or master open and 5 ms for each
call over a live control socket, and runs the remote command locally against
the canned zfs. Cases: plain `-O localhost -T localhost` (one shared master),
doas `-O 'localhost doas' -T localhost` (two masters), and `-z` with the plain
specs; 25 children, four snapshots each, median of 3 warmed runs, all three
refs alternating in one session. Seconds, with the ratio to
`upstream-compat-final`:

| Handshake | Case | Scenario | Before (wave 3) | After | Upstream |
| ---: | --- | --- | ---: | ---: | ---: |
| 80 ms | plain | No-op | 0.587, 1.24 | 0.415, 0.87 | 0.475 |
| 80 ms | plain | Incremental | 1.904, 0.72 | 1.387, 0.53 | 2.636 |
| 80 ms | doas | No-op | 0.607, 1.22 | 0.450, 0.90 | 0.498 |
| 80 ms | doas | Incremental | 2.042, 0.75 | 1.563, 0.58 | 2.715 |
| 80 ms | `-z` | No-op | 0.643, 1.30 | 0.461, 0.93 | 0.495 |
| 80 ms | `-z` | Incremental | 2.100, 0.76 | 1.580, 0.57 | 2.776 |
| 200 ms | plain | No-op | 0.972, 1.33 | 0.559, 0.77 | 0.730 |
| 200 ms | plain | Incremental | 2.531, 0.87 | 1.534, 0.53 | 2.921 |
| 200 ms | doas | No-op | 1.012, 1.33 | 0.595, 0.78 | 0.760 |
| 200 ms | doas | Incremental | 2.751, 0.92 | 1.713, 0.57 | 2.990 |
| 200 ms | `-z` | No-op | 1.031, 1.37 | 0.598, 0.79 | 0.754 |
| 200 ms | `-z` | Incremental | 2.741, 0.90 | 1.712, 0.56 | 3.047 |
| 0 | plain | No-op | 0.291, 1.41 | 0.275, 1.32 | 0.207 |
| 0 | plain | Incremental | 1.088, 0.59 | 0.972, 0.52 | 1.856 |
| 0 | doas | No-op | 0.291, 1.37 | 0.290, 1.36 | 0.213 |
| 0 | doas | Incremental | 1.230, 0.64 | 1.123, 0.58 | 1.923 |
| 0 | `-z` | No-op | 0.320, 1.55 | 0.305, 1.47 | 0.207 |
| 0 | `-z` | Incremental | 1.268, 0.63 | 1.159, 0.58 | 2.001 |

A plain remote no-op now makes 7 ssh invocations (support probe, one master,
four multiplexed calls, one exit; upstream makes 9), where it made 5 before,
four of them direct handshakes. With no handshake cost the no-op is still
slower than upstream: the rest is endpoint-context and `-z` requote forks in
startup plus run-root setup. Local runs do not reach this code; `ab.py` in the
same session (current / upstream, ratio): 25-child no-op 0.095 / 0.146, 0.65;
incremental 0.664 / 1.391, 0.48; 100-child no-op 0.092 / 0.143, 0.64;
incremental 2.619 / 5.617, 0.47. The local-only micro-bench counts are
unchanged.

### Wave 3 simplification (2026-09-23)

The run root records its identity, owner, and mode when it is created, so
removal runs no `id`; TMPDIR validation reads owner and mode from one
`ls -ldn`. The `-V` clock reads `date` once per sample. `-d` planning reads
all creation times of a delete plan in one batched `zfs get` and one `awk`
pass. Remote capabilities keep one slot per role and capture probe stdout in
the shell. Forwarded `-k` aliases are read at most once per run.

`tests/run_microbench.sh -V` helper totals, 25x4 / 8x2 fixtures: no-op 31 ->
22 / 31 -> 22, incremental dry run 10 -> 7 / 10 -> 7, incremental 147 -> 131
/ 96 -> 80 (`id` 1 -> 0; `date` falls the most, for example no-op 17 -> 9).
Without `-V` the 25x4 runs spawn 13 (no-op), 4 (dry run), and 111
(incremental) helpers.

`ab.py` against `upstream-compat-final`, same method as below (current /
upstream, ratio): 25-child no-op 0.113 / 0.183, 0.62; incremental 0.806 /
1.708, 0.47; 100-child no-op 0.113 / 0.179, 0.63; incremental 3.052 / 6.636,
0.46.

With `-O localhost -T localhost` through a local mock ssh (25 children, median
of 3, same session): no-op 0.334 s after wave 3 vs 0.525 s after wave 2 and
0.246 s upstream; incremental 1.252 s vs 1.461 s and 2.132 s upstream. The
remote no-op is still about 1.3-1.5 times slower than upstream; the remaining
cost is in remote destination discovery and run-root setup. ssh argv is
unchanged.

### Wave 2 simplification (2026-09-23)

Startup resets each owner once and looks helpers up by walking the secure PATH
in the shell; the replication driver, send/receive rendering, and ssh
transport run in the current shell instead of per-dataset command
substitutions; discovery keeps its lists in files, reuses the no-op proof's
destination listing, and probes destination existence only after a failed
listing.

`tests/run_microbench.sh -V` helper totals, 25x4 / 8x2 fixtures: no-op 33 ->
31 / 33 -> 31, incremental dry run 12 -> 10 / 12 -> 10, incremental 167 -> 147
/ 116 -> 96 (`uname` 3 -> 1, incremental `rm` 16 -> 7). Without `-V` the 25x4
incremental run spawns 112 helpers (129 before) and the no-op run 14. A bash 5
xtrace fork count of the canned no-op run falls from 84 to 31 (startup 61 ->
19). The canned 25-child incremental run issues 83 zfs calls (85 before).
`command_render_calls` rose from 1 to 53 on that run only because renders now
happen in the counted shell.

`ab.py` alternating A/B against `upstream-compat-final` on macOS (`/bin/sh` is
bash 3.2), canned ZFS, four snapshots per dataset, five warmed repetitions,
median elapsed seconds (current / upstream, ratio):

| Children | Scenario | After wave 1 | After wave 2 |
| ---: | --- | ---: | ---: |
| 25 | No-op | 0.198 / 0.175, 1.13 | 0.110 / 0.144, 0.76 |
| 25 | Incremental | 1.347 / 1.627, 0.83 | 0.696 / 1.393, 0.50 |
| 100 | No-op | 0.198 / 0.174, 1.14 | 0.112 / 0.150, 0.75 |
| 100 | Incremental | 4.805 / 6.554, 0.73 | 2.716 / 5.666, 0.48 |

The columns come from different sessions, so compare ratios rather than
absolute times across columns. Both the no-op and incremental paths are now
faster than upstream on this fixture.

### Wave 1 simplification (2026-09-23)

Each dataset's snapshots are now planned in one `awk` pass (common snapshot,
transfer list, divergence, and the `-d` delete list), and the destination
existence cache lookup is linear instead of quadratic. Runtime scratch files
are created in the current shell, CLI-token quoting no longer uses command
substitutions, printable failure-report tokens are quoted without `awk` or
`sed`, profiling reads its start clock only when argv has a `-V` cluster, and
process start-token normalization no longer forks.

`tests/run_microbench.sh -V` helper totals, 25x4 / 8x2 fixtures: no-op 39 ->
33 / 39 -> 33, incremental dry run 12 / 12 (unchanged), incremental 247 -> 167
/ 162 -> 116. Without `-V` the 25x4 incremental run spawns 129 helpers (181
before). The `sed` and `awk` rows fell the most (incremental `awk` 130 -> 64
with `-V`).

`ab.py` alternating A/B against `upstream-compat-final` on macOS (`/bin/sh` is
bash 3.2), canned ZFS, four snapshots per dataset, five warmed repetitions,
median elapsed seconds (current / upstream, ratio):

| Children | Scenario | Before wave 1 | After wave 1 |
| ---: | --- | ---: | ---: |
| 25 | No-op | 0.224 / 0.164, 1.37 | 0.198 / 0.175, 1.13 |
| 25 | Incremental | 1.594 / 1.398, 1.14 | 1.347 / 1.627, 0.83 |
| 100 | No-op | 0.203 / 0.152, 1.33 | 0.198 / 0.174, 1.14 |
| 100 | Incremental | 6.468 / 5.689, 1.14 | 4.805 / 6.554, 0.73 |

The "before" column is an earlier session (median of three), so compare the
ratios rather than absolute times across columns. Incremental replication is
now faster than upstream on this fixture; the no-op path is still about 24 ms
slower. A 400-child incremental run fell from 100.8 s to 28.5 s with the
linear existence cache alone.

### September 2026 simplification

The simplification keeps the enhancement set from `optimize/performance`
while removing generic job/operation state, property handoff objects, and
repeated staging and rendering. The source tree is about 17% smaller than
that branch. Source discovery uses registered helper PIDs; send/receive has
one small registry with completion files. Failure cleanup retains process
groups after their leaders exit, and property metadata publication retains
detected-failure rollback for its two files. Cached property lookup uses a
direct row-reading loop: it preserves first-match precedence and encoded
values while avoiding expensive whole-table shell pattern matching.

The native, instrumented micro-benchmark now observes these helper totals:

| Fixture | No-op | Incremental dry run | Incremental |
| --- | ---: | ---: | ---: |
| 25 children, 4 source snapshots each | 39 | 12 | 247 |
| 8 children, 2 source snapshots each | 39 | 12 | 162 |

These are `-V` runs of `tests/run_microbench.sh`, with its 22-helper inventory.
They measure subprocess work, not elapsed transfer time. The corresponding
24 decreases in `tests/perf_budgets.tsv` retain 15% headroom and never raise a
ceiling. The baseline timing comparison below uses uninstrumented runs so
the counting wrappers cannot distort the ranking.

#### Comparison with upstream-compat-final

On macOS, three alternating, warmed repetitions per case compared the current
source with `upstream-compat-final` at
`49a0f40e7d18ab0d7b6ecab5fafbc85282dc004f`. Both refs received the same canned
ZFS state: four source snapshots per dataset, with either all four already
present or the final snapshot pending at the destination. Child counts
exclude the root, so incremental cases perform exactly 26 or 101 sends and
receives. The `-P` fixtures have matching properties and require no property
mutations. Every run checked successful status, read coverage, incremental
base, destinations, and operation counts.

That session measured the current source 40-50 ms slower than upstream on
no-op runs, 1-6% slower on plain incremental runs, and 82-83% faster on
incremental runs with `-P`. Those figures are superseded by the wave 4
comparison above, where every local case, `-P` included, is faster than
upstream on the same fixture shape.

A separate sensitivity run added a fixed 50 ms delay to each mock `list` and
`get` call, with three alternating repetitions at 25 children. No-op fell
from 0.32 s to 0.23 s (28% faster), and plain incremental fell from 2.90 s to
1.44 s (50% faster). The delay includes the sleep process and allows the
normal read concurrency; it is a simulation, not a measured disk or network
cost. The exact latency at which the ranking changes was not measured.

The fixtures adapt the old branch's name-only and per-child queries from the
same GUID-bearing tables. Direct mock dispatch was checked against 283
responses by exact bytes and status, avoiding a linear fixture lookup cost
that would bias the comparison. Each freshly written mock was warmed outside
the timed interval, and its log cleared before measurement. Runs were
serialized, capped at 180 seconds each and 900 seconds for the main batch;
the sensitivity limits were 30 and 120 seconds, with five seconds allowed for
timeout cleanup. The native helper-count runs above were separate.

These offline measurements cover shell and control overhead with canned send
streams. They do not measure actual ZFS transfer throughput, peak memory,
or other platforms. Real ZFS performance remains a manual disposable-guest
check, using the compatible FreeBSD cases documented below and in
[`docs/testing.md`](docs/testing.md). Raw commands,
observations, operation checks, and source hashes were retained with the
review's local benchmark artifacts.

Validation for this review: `./tests/validate.sh full` passed the pinned lint
stack, all 38 shunit suites, and the report-only bash-xtrace run. Approximate
line coverage was 96.56% (8,376 of 8,674 executable lines). Targeted property
lookup and cleanup-helper checks also passed under `dash`; its two real
process-group producer cases were skipped because that shell had no verified
group-isolation mode on this host. Those cases passed under the host shell.
No real ZFS or VM integration was run.

### Earlier optimization phases

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

2026-09-01 re-measurement. The counted tool list above missed the hot path:
it never included `cat`, `rm`, `id`, or `stat`, so the per-dataset work of a
live incremental run was invisible while every budgeted tool stayed flat. The
micro-bench now counts 22 helpers and has a third scenario, `incr` (live
incremental, one receive per dataset), which is the path real syncs
exercise. Against the same canned fixture, `upstream-compat-final` completes
that scenario in about 2.1 s with 301 helper spawns; this branch took 14 s
and 2,375 spawns before the fixes below and 7 s / 1,129 spawns after them:

| Scenario (25x4 fixture, 22 counted tools) | Before | After |
| --- | --- | --- |
| `incr` helper spawns | 2,375 | 1,129 |
| `incr` recursive destination listings | 28 | 3 |
| `incr` `stat` + `id` spawns | 860 + 430 | 2 + 1 |
| `noop` helper spawns | 88 | 51 |

The two fixes: the per-run temp root is no longer revalidated with `id` and
`stat` on every artifact allocation and cleanup (only at creation and before
the whole-root `rm -rf`), and the batched live destination view is captured
once per pass with per-dataset dirty marks instead of being re-listed for the
whole tree after every receive. A third change then removed the per-dataset
reconcile churn: snapshot record lists are normalized in the shell instead of
through `tr`, per-dataset record captures use command substitution instead of
a staged file plus `cat` and `rm`, per-record `$(...)` extraction loops became
parameter expansion, and the last-common, divergence, shared-name, and
live-range scans became one POSIX awk pass per dataset. `incr` fell from
1,129 to 231 helper spawns and from about 7 s to 2.2 s on the canned
fixture, close to `upstream-compat-final` (2.1 s, 301 spawns) in that session
while keeping guid-aware matching and the divergence contract. A later
alternating A/B still measured it 1.14 times slower than upstream on
incremental runs and 1.33-1.37 times slower on no-op runs; see the wave 1
results above.

Remote and structural results:

- Cold incremental `-O` pull: 12 -> 10 ssh invocations; control-socket
  `-O check` probes around commands: 2 -> 0 (one `-M` master open per remote
  role and distinct host spec, multiplexed, one `-O exit` close at exit). One
  capability probe round trip per host per run.
- Every `-O`/`-T` run opens each remote role's control master before its
  first remote command: one master per distinct host spec, and under
  `BatchMode=yes` two masters overlap their handshakes. The capability probe,
  source listing, and destination discovery then multiplex over it. A remote
  no-op costs one or two handshakes instead of four direct connections; this
  is pinned by black-box tests.
- Background jobs: 21 -> 5 helper spawns per send/receive job
  (supervision-lite: setsid process group + one status file, runner module
  deleted).
- Structural size: the property-cache module and background-job runner module
  were deleted outright. Path security, lock coordination, and runtime
  artifacts are current concern-specific modules; sensitive caller counts
  ratchet down in `tests/budget_policy.tsv`.

Recursive property-prefetch grouping was measured again during the module
ownership refactor. The two required recursive `zfs get` views are unchanged,
but their two grouping passes plus merge are now one POSIX `awk` pass and the
staging group fell from seven artifacts to five. That historical offline
comparison used deterministic 100- and 1,000-dataset fixtures, alternating
samples, exact byte comparison, and peak-RSS measurement when supported.
The September property simplification replaced both grouping implementations
with the shared normalization program and removed their comparison-only
benchmark. The earlier seven-sample medians on macOS were:

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
- Phase 2 — render commands once. Direct commands keep their execution argv
  and render diagnostics only when needed. Send/receive pipelines build one
  shell-quoted execution string and reuse it for verbose diagnostics, so
  displayed arguments preserve the same boundaries as execution.
- Phase 3 — cache flattening. Snapshot record lookups serve from flat
  per-run record files; the per-dataset property cache-object module was
  deleted in favor of in-memory property tables with targeted invalidation;
  destination existence answers use an O(1) prepend-only cache; backup
  metadata uses a validate-once buffer.
- Phase 4 — per-dataset dirty live rechecks. Live destination rechecks are
  served from one batched recursive destination listing captured at most
  once per replication pass; a receive, rollback, or snapshot destroy marks
  only the mutated dataset dirty, and
  a dirty dataset's later rechecks are depth-1 live listings of that dataset
  alone. `-Y` drops the view and the dirty list at pass boundaries. Rechecks
  still happen before every mutating decision; they are just no longer
  per-dataset round trips, and a mutation no longer re-lists the whole
  destination tree before the next dataset's receive. Dataset creation and
  property set/inherit do not change snapshot rows and do not dirty the view.
- Phase 5 — direct send-job scheduling. The send-job module keeps one
  registry row and one status file per job, checks all jobs for completion,
  and owns failure cleanup. Verified process-group isolation is preferred;
  the fallback wrapper lists the full process table (`ps -A`) only during
  abort, so cron-launched runs can also discover descendants. Source
  discovery uses its helper PID directly, without send-job status metadata
  or a completion FIFO.
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
removal, leaving only the single hardened rendered-shell execution site), 22
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
  name distinct remote contexts. The two control-master handshakes already
  overlap under `BatchMode=yes`; the capability probes still run one after
  the other. Needs a checked role-state handoff because subshells cannot
  mutate parent globals; never publish partially initialized role state.
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
- C6. Broaden the existing target-discovery batch with bounded internal
  fanout for other independent read-only remote metadata. Preserve its
  ordered protocol, status validation, and empty-on-failure outputs.

Other remaining items:

- Broader remote collector (remaining part of original item 2): the target
  dataset/snapshot batch and its fail-closed local streaming are complete.
  A future collector could add selected property tables or combine compatible
  helper/OS discovery without a remote install, but must retain exact framing,
  fail closed on truncation, and never persist remote state across runs.
- Property-read scoping (item 6): build the minimum safe property set from
  active options instead of `zfs get ... all` where the mode provably does
  not need full property discovery; fall back to `all` whenever
  completeness cannot be proven.
- Multi-line property values in a recursive read: the name-list parser's
  trusted run ends at the first ambiguous record, so every later dataset
  falls back to per-dataset reads (3.9 s instead of 1.5 s for one multi-line
  value in a 25-child `-P -R` run). Restarting the run at each dataset's
  first record would confine the fallback to the affected dataset; it needs a
  security review, because a fake record could then sit in any earlier
  ambiguous value rather than only the one just before it.
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
- Remote no-op at zero latency: at parity with upstream since 2026-09-26
  (ratio 1.00 and 0.97 in two runs of `tests/run_perf_ab.sh --baseline-ref
  upstream-compat-final --sizes 25 --latency-ms 0 --reps 7`; 1.25 in the
  wave 4 comparison). To get ahead of it: the wave 4 sweep's profiling puts
  most of the rest in the size of the remote capability-probe and
  discovery-batch programs; the probe's stdout capture (one command
  substitution per probe) and the cleanup wrapper path lookup still fork.
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

- `tests/run_microbench.sh [-V] [--forks] [-d N -s S] [noop|dryrun_incr|incr|remote_noop|remote_incr|props ...]` —
  helper-spawn counts (22 counted tools) and `-V` profile counters against
  the canned zfs; `incr` is the live per-dataset hot path and `props` the
  same with `-P` and 68 matching properties per dataset. The remote
  scenarios run `-O localhost -T localhost` through a socket-aware mock ssh.
  Every scenario adds `ssh_connections`, `ssh_invocations` and
  `ssh_master_opens` rows; the connection and master rows are pinned
  exactly, in both directions. The work directory sits under `/tmp`, so
  counts do not depend on `TMPDIR`. Budgets in `tests/perf_budgets.tsv` are
  enforced by `tests/test_zxfer_microbench_budgets.sh`. To compare against
  another launcher on the same fixture, point `ZXFER_MOCKBIN_ZXFER_BIN` at it.
  `--forks` (bash 4.1 or later, or set `ZXFER_MICROBENCH_BASH`) reruns each
  scenario under xtrace and adds advisory `forks_{startup,run,exit,total}`
  subshell counts; they are advisory only and never budgeted.
- `tests/run_perf_ab.sh --baseline-ref REF [--candidate-root DIR] [--sizes 25,100] [--snapshots 4] [--scenarios LIST] [--shell PATH] [--reps N] [--summary FILE] [--latency-ms 80]` —
  an advisory wall-clock A/B on the canned zfs, with no ZFS and no root. REF
  is any commit-ish, a SHA included. For each size it runs noop, incr,
  remote_noop and remote_incr (or the `--scenarios` list, which may add the
  opt-in `props`: incr with `-P` and 68 matching properties per dataset):
  one warm-up, then alternating runs. `--snapshots` sets the fixture depth
  and `--shell` the interpreter of both launchers (for example
  `/bin/dash`). It prints median/min/max and the candidate/baseline ratio as
  TSV, plus an optional appended Markdown table, and exits non-zero only on
  harness or usage errors. It replaces the scratch `ab.py` and `abot_lat.py`
  harnesses used in earlier waves.
- `tests/test_contract_failures.sh` fails every zfs and ssh call of nine
  small scenarios in turn and requires zxfer to fail closed (see
  `docs/testing.md`). An optimization that changes call order or error
  handling must keep it green.
- `tests/run_perf_compare.sh` and the VM `perf-compare` layer compare this
  branch against a baseline ref inside the same disposable guest:

  ```sh
  ./tests/run_vm_matrix.sh --profile smoke --test-layer perf
  ZXFER_VM_PERF_BASELINE_REF=upstream-compat-final \
    ZXFER_VM_PERF_CASES="chain_local_noop chain_local_incr fanout_local_j1_incr" \
    ./tests/run_vm_matrix.sh --profile local --guest freebsd --test-layer perf-compare
  ```

  This older baseline needs BSD userland and cannot run every current
  property case. See [the supported comparison cases](docs/testing.md)
  before expanding that selection.
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
