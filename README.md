zxfer
=====

`zxfer` is a POSIX shell tool for high-reliability ZFS snapshot replication
across local and remote hosts. This maintained fork focuses on safer
replication behavior, better portability, stronger failure reporting, and
faster handling of large dataset trees.

It targets current OpenZFS 2.0+ workflows on maintained FreeBSD branches,
Linux/OpenZFS, OmniOS/illumos, and OpenZFS-on-macOS. The command is meant for
production administrators, so CLI behavior, operator-visible output, and
replication semantics are treated as public interfaces.

Before using it against production data, validate the exact command line on
throwaway datasets, sparse-file pools, or a disposable VM. Options such as
`-d`, `-F`, migration modes, and property restore flows can be destructive if
pointed at the wrong destination.

For the full CLI reference, use:

```sh
man zxfer
```

Bundled references:

- [man/zxfer.8](./man/zxfer.8) for FreeBSD/Linux-style installs
- [man/zxfer.1m](./man/zxfer.1m) for Solaris/illumos-style installs
- [docs/cli-examples.md](./docs/cli-examples.md) for task-oriented examples

If you are upgrading from the 2019 `v1.1.7` release, start with
[docs/whats-new-since-v1.1.7.md](./docs/whats-new-since-v1.1.7.md).

## Branch Guide

- `main`: active development branch for this fork; all new work merges here
- `upstream-compat-final`: historical branch from this fork before
  rsync-mode removal and before the later breaking divergence on `main`
- `upstream-archive`: reference branch that mirrors the latest imported upstream
  [allanjude/zxfer](https://github.com/allanjude/zxfer) history

If you need the old rsync-capable code path, start by reviewing
`upstream-compat-final` and `upstream-archive` instead of assuming `main`
preserves pre-removal behavior. For the full historical context, see
[docs/upstream-history.md](./docs/upstream-history.md).

## Quick Start

Replicate a local recursive dataset tree:

```sh
./zxfer -v -R tank/data backup/data
```

Pull snapshots from a remote host:

```sh
./zxfer -v -O user@example.com -R zroot backup/zroot
```

Repeat until the destination converges:

```sh
./zxfer -v -Y -R tank/src backup/dst
```

Use remote compression:

```sh
./zxfer -v -z -T backup@example.com -R tank/src backup/dst
```

## Highlights

- POSIX `/bin/sh` implementation with no Bash dependency
- Recursive and non-recursive snapshot replication
- Local and remote replication with `-O` and `-T`
- Wrapper-style remote host specs such as `user@host pfexec` or `user@host doas`
- Concurrent send/receive jobs with explicit per-dataset source discovery and
  supervision-lite job teardown (process-group signaling plus per-job status
  files) via `-j`
- Property replication, overrides, and unsupported-property skipping for the
  current OpenZFS 2.0+ support floor
- Property backup and restore with `-k` and `-e`, using hardened metadata
  storage outside dataset mountpoints and the current `#format_version:2`
  schema
- Optional raw sends with `-w`
- Optional `zstd` compression with `-z` or a custom `zstd` compressor command
  with `-Z`
- Structured stderr failure reports with default command-field redaction,
  optional `ZXFER_ERROR_LOG` mirroring, and an explicit
  `ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1` local-debug override
- Per-run ssh control sockets and one in-memory remote capability probe per
  host per run; all run-private temp state lives under one 0700 per-run temp
  root removed in one pass at exit. The only cross-process lock left is the
  `ZXFER_ERROR_LOG` append lock (slim pid+start-token metadata, validated
  stale-owner reaping, checked release)
- Identity-aware recursive snapshot discovery with `name,guid` records, plus a
  fast clean-no-op proof for eligible recursive runs — local sources and
  remote-origin pulls alike
- Batched remote-target destination discovery for `-T`, so destination dataset
  inventory, missing-root pool probing, and destination snapshot listing share
  one target-side ssh shell invocation

## Useful Options

- `-j jobs`: run concurrent send/receive jobs; when `jobs > 1`, zxfer uses
  explicit per-dataset source discovery instead of the serial recursive
  listing (the clean no-op proof still runs one serial recursive stream
  first). Source discovery runs as a tracked background helper with staged
  stderr and PID cleanup, while send/receive workers run supervision-lite:
  each job is one backgrounded job shell that writes its own status file,
  and aborts signal the job's setsid process group (or its tracked child
  set) instead of a bare wrapper PID. zxfer also serializes conflicting ancestor/descendant
  destination receives on the same target so parent and child datasets do not
  receive concurrently, and its ready queue skips blocked descendants to start
  later independent datasets while job slots remain. Local-origin and
  remote-origin runs require a resolved `parallel` helper on the executing
  origin host; zxfer intentionally checks only that the helper exists through
  the secure-PATH model, so operators and packages must provide an
  implementation compatible with the GNU Parallel-style options used by the
  rendered source-discovery pipeline
- `-V`: enable very verbose debug output and end-of-run profiling counters,
  including startup latency, trap-cleanup timing, per-phase listing times,
  ssh/zfs invocation counts, runtime temp-file counts, and live destination
  snapshot recheck counts (counter keys are stable; counters for deleted
  machinery read 0)
- `-x pattern`: exclude datasets from recursive replication
- `-Y`: repeat replication until no sends or destroys are performed, or until
  the built-in iteration cap is reached
- `-z`: compress ssh send/receive streams with `zstd`
- `-Z "command"`: replace the default `zstd` compressor command with a custom
  variant such as `zstd -T0 -3`

For `-O`, `-T`, and `-Z`, zxfer treats the option value as literal
whitespace-delimited argv tokens. Outer shell quoting is fine, but embedded
quote characters or backslash escapes inside the value are rejected instead of
being silently re-tokenized.

See the man pages and [docs/cli-examples.md](./docs/cli-examples.md) for the
full option set and additional workflows.

## Supported Platforms

zxfer is intended to work with current OpenZFS 2.0+ environments:

- FreeBSD 14.4+ and 15.0+ maintained branches with OpenZFS
- Linux with OpenZFS
- currently supported OmniOS / illumos systems
- current OpenZFS on macOS workflows

For releases published after 2026-05-01, zxfer follows maintained FreeBSD
branches. The current FreeBSD baseline is 14.4+ on the stable/14 line and
15.0+ on the stable/15 line. FreeBSD 14.3 and older releases are outside this
baseline; end-of-life FreeBSD releases are not supported. Pre-OpenZFS 2.0
behavior, Solaris Express-era property profiles, and older backup metadata
layouts are intentionally unsupported.

It also supports VM-backed validation from Linux, macOS, and WSL2 hosts through
[tests/run_vm_matrix.sh](./tests/run_vm_matrix.sh).

Platform caveats, host layouts, and compatibility notes live in
[docs/platforms.md](./docs/platforms.md).

## Operational Notes

zxfer rebuilds `PATH` from a trusted allowlist and resolves required helpers to
absolute paths. Remote `zfs`, `cat`, `parallel` for `-j > 1`, and compression
helpers are resolved per host instead of assuming the same binary path exists
everywhere.
Local-only runs do not resolve `ssh`; it is required when `-O` or `-T` needs a
remote transport.

zxfer-managed ssh connections default to `BatchMode=yes` and
`StrictHostKeyChecking=yes`. Use `ZXFER_SSH_USER_KNOWN_HOSTS_FILE` to pin an
absolute known-hosts file, or `ZXFER_SSH_USE_AMBIENT_CONFIG=1` if you need to
fall back to the ambient local ssh policy.

SSH control sockets and remote capability state are per-run only. Each
invocation opens at most one control master per remote role under its private
per-run temp directory, multiplexes its own remote commands over that socket,
and closes it on exit; clean no-op runs never open a master at all. Remote
helper discovery costs one capability probe round trip per host per run, held
in memory and identity-checked against the host spec, secure PATH, ssh
policy, and requested helper set -- nothing is shared between concurrent or
consecutive zxfer invocations, matching upstream zxfer behavior. Only
`ZXFER_ERROR_LOG` appends still coordinate through a metadata-bearing lock
directory that records the owner PID and process-start identity; zxfer
validates and reaps stale or corrupt owners before reuse and checks release
operations instead of silently suppressing failures.

Long-lived parallel send/receive work runs supervision-lite: each job is one
backgrounded job shell (in its own `setsid` process group when the host
provides it) that writes its own exit status to a per-run status file and
notifies the rolling completion queue itself. Trap-time abort signals the
job's process group or its tracked direct children — never a bare wrapper
PID — waits briefly, escalates once with KILL, and only then reaps; an
un-reaped child's PID/PGID cannot be recycled, so the signal cannot reach an
unrelated process. A missing or non-numeric status file at wait time is
reported as a job failure. The same checked-cleanup rule applies to ssh
control-socket teardown during trap cleanup: if zxfer cannot close a managed
socket after otherwise successful work, it exits nonzero instead of
reporting a clean run.

For `-j` send/receive work, the scheduler also treats ancestor/descendant
destination datasets on the same target as mutually exclusive. zxfer skips
blocked descendants and starts later independent datasets while job slots
remain, waiting for a conflicting receive only when no pending dataset is ready
to run. Recursive parent/child destination trees therefore no longer race each
other and degrade later into truncated-stream collateral failures.

Recursive snapshot discovery remains identity-aware: initial source and
destination snapshot records carry `name,guid` so a same-name snapshot with a
different GUID cannot be treated as a clean match. For eligible `-R` runs —
local sources and `-O` pulls alike — with a local destination and no snapshot
creation, property, migration, restore, backup, or target-host work, zxfer
first tries a fast no-op proof. That proof compares one recursive source
`name,guid` stream with one normalized destination `name,guid` stream staged
under the per-run temp root and falls back to full discovery when the streams
differ or the destination is missing; a proven clean no-op skips the
creation-order source listing and the destination existence check entirely.
`-U` and `-g` can remain enabled on this proof path because exact no-op
discovery leaves no source transfer queue, destination delete queue, or
property/create work to consume those checks.

When a destination snapshot shares a source snapshot's name but carries a
different GUID, the destination has diverged under identical names and
converging it is destructive. zxfer always prints a warning on stderr (not
gated on `-v`/`-V`) naming the dataset, the diverged-snapshot count, and up to
three example snapshots with both GUIDs. The destructive convergence —
destroying the diverged destination snapshots, rolling back to the last
GUID-matching common snapshot, and resending the source range over them — runs
only when BOTH `-d` and `-F` are active. Without both flags the run fails
closed with a structured error naming the diverged dataset, and zero deletes
or sends are planned for it. After a converged dataset's receive completes,
zxfer re-checks the live destination listing and aborts with a precise error
naming the snapshot if any name-match/GUID-mismatch remains, so an external
writer re-diverging the destination surfaces as an explicit failure instead of
a silent destroy-and-resend loop. With `-V`, planning prints one
`Last common snapshot: ...; diverged destination snapshots: N.` line per
planned dataset and the profile summary reports `diverged_snapshot_warnings`.

When `-T` is used, destination discovery runs a structured target-side batch:
recursive destination dataset inventory, the missing-root pool fallback probe,
and destination snapshot listing are issued inside one remote `sh -c` payload.
Large snapshot stdout is streamed back as `name,guid` records, while status and
stderr sections are staged and parsed locally. The local destination path keeps
the direct `zfs` command flow.

Short-lived local background helpers that still need shell wrappers, such as
progress dialogs and delete-planning identity writers, register their PIDs
with the in-memory runtime cleanup tracker. The remaining local wrapper-style
helpers run under a small TERM-aware child wrapper so early-exit cleanup no
longer falls back to signaling a bare wrapper-shell PID.

Current runtime caveats are tracked in [KNOWN_ISSUES.md](./KNOWN_ISSUES.md).

## Testing

Run the main local validation steps:

```sh
./tests/run_shunit_tests.sh
./tests/run_lint.sh
ZXFER_COVERAGE_MODE=bash-xtrace ./tests/run_coverage.sh
```

For unattended integration coverage on a disposable guest boundary, prefer:

```sh
./tests/run_vm_matrix.sh --profile smoke
```

For manual, non-gating throughput checks inside a disposable guest, use:

```sh
./tests/run_vm_matrix.sh --profile smoke --test-layer perf
```

To compare the current checkout against `upstream-compat-final` before
performance work, keep the run VM-backed:

```sh
ZXFER_VM_PERF_BASELINE_REF=upstream-compat-final ./tests/run_vm_matrix.sh --profile smoke --test-layer perf-compare
```

Use [tests/run_integration_zxfer.sh](./tests/run_integration_zxfer.sh)
directly only when you explicitly want the manual host-side harness on a
disposable ZFS-capable system.

Full test-layer guidance, performance-harness usage, safety notes, coverage
details, and CI workflows live in [docs/testing.md](./docs/testing.md).

## Documentation

- [docs/README.md](./docs/README.md): documentation index
- [docs/whats-new-since-v1.1.7.md](./docs/whats-new-since-v1.1.7.md): operator-focused upgrade guide from the legacy 2019 release
- [docs/platforms.md](./docs/platforms.md): platform support and compatibility notes
- [docs/testing.md](./docs/testing.md): unit, coverage, integration, and manual performance workflows
- [docs/troubleshooting.md](./docs/troubleshooting.md): common failures and debugging hints
- [docs/architecture.md](./docs/architecture.md): module layout and replication flow
- [examples/README.md](./examples/README.md): runnable command templates
- [CHANGELOG.txt](./CHANGELOG.txt): release history
- [KNOWN_ISSUES.md](./KNOWN_ISSUES.md): open issues
- [SECURITY.md](./SECURITY.md): security model and reporting guidance
- [CONTRIBUTING.md](./CONTRIBUTING.md): contributor workflow

## Project Status

- Active maintained fork focused on reliability, portability, and testability
- Legacy rsync mode (`-S`) has been removed
- Issues and pull requests are welcome

## Acknowledgements

Thanks to the original authors, contributors, and operators who have continued
to use and validate zxfer across multiple ZFS platforms.
