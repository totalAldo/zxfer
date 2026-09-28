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

- canonical [man/zxfer.8](./man/zxfer.8) for section 8 installs
- generated [man/zxfer.1m](./man/zxfer.1m) for Solaris/illumos-style installs
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
  per-job status files and bounded process cleanup via `-j`
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
  host and requested tool set per run (equal `-O` and `-T` specs share it);
  all run-private temp state lives under one 0700
  per-run temp root removed in one pass at exit (only the control sockets
  move to a short private directory of their own when a long `TMPDIR` would
  push their path past the socket path limit), and zxfer takes no
  cross-process locks (each `ZXFER_ERROR_LOG` report is one validated
  append-mode write)
- Identity-aware recursive snapshot discovery with `name,guid` records, plus a
  fast clean-no-op proof for eligible recursive runs — local sources and
  remote-origin pulls alike
- One destination discovery path for local and `-T` destinations: plain
  `zfs list` calls, multiplexed over the target's ssh control master under
  `-T`

## Useful Options

- `-j jobs`: run concurrent send/receive jobs; when `jobs > 1`, zxfer uses
  explicit per-dataset source discovery instead of the serial recursive
  listing (the clean no-op proof still runs one serial recursive stream
  first). Source discovery runs as a tracked background helper with staged
  stderr and PID cleanup. Send/receive jobs write their own status files;
  abort uses a verified process group or a descendant-tracking wrapper.
  zxfer also serializes conflicting ancestor/descendant
  destination receives on the same target so parent and child datasets do not
  receive concurrently, and its ready queue skips blocked descendants to start
  later independent datasets while job slots remain. Local-origin and
  remote-origin runs require a resolved `parallel` helper on the executing
  origin host (without `-O`, the local helper is checked at startup, even for
  a run with nothing to send); zxfer intentionally checks only that the helper
  exists through the secure-PATH model, so operators and packages must provide
  an implementation compatible with the GNU Parallel-style options used by the
  rendered source-discovery pipeline
- `-V`: enable very verbose debug output and end-of-run profiling counters,
  including startup latency, trap-cleanup timing, per-phase listing times,
  ssh/zfs invocation counts, runtime temp-file counts, and live destination
  snapshot recheck counts (counter keys are stable; counters for deleted
  machinery read 0)
- `-x pattern`: exclude datasets from recursive replication
- `-Y`: repeat replication until no sends or destroys are performed, or until
  the built-in iteration cap is reached; each pass lists the destination
  afresh, so a destination that another tool changed after one pass's
  discovery is seen by the next pass
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
absolute paths. Remote helpers are resolved per host instead of assuming the
same binary path exists everywhere: `zfs` on every remote host, `parallel` on
the origin for `-j > 1`, `cat` on the origin for `-e` and on the target for
`-k`, and the compression helpers for `-z`. Destination discovery on a `-T`
target needs nothing but `zfs`.
Local-only runs do not resolve `ssh`; it is required when `-O` or `-T` needs a
remote transport.

zxfer-managed ssh connections default to `BatchMode=yes` and
`StrictHostKeyChecking=yes`. Use `ZXFER_SSH_USER_KNOWN_HOSTS_FILE` to pin an
absolute known-hosts file, or `ZXFER_SSH_USE_AMBIENT_CONFIG=1` if you need to
fall back to the ambient local ssh policy.

SSH control sockets and remote capability state are per-run only. Each
invocation opens at most one control master per remote role under its private
per-run temp directory, opens it before its first remote command (a `-T` host
spec equal to the `-O` spec shares the origin master), multiplexes every
remote command of the run over it, and closes it on exit. When a long
`TMPDIR` would push a socket path past the `sun_path` limit (about 104
bytes), the sockets go to a private `zxfer.ssh.XXXXXX` directory under the
first safe default temp directory (`/dev/shm`, `/run/shm`, then `/tmp`)
instead, which zxfer removes at exit. Remote
helper discovery costs one capability probe round trip per host and
requested tool set per run (a `-T` spec equal to the `-O` spec shares the
origin's probe), held in memory and keyed by the host spec and requested
helper set (the secure PATH and ssh policy are fixed for the run).
A recursive `-O -j` pull that the fast no-op proof finds work for probes the
origin a second time, for `parallel`. Nothing is shared between
concurrent or consecutive zxfer invocations, matching upstream zxfer behavior.
`ZXFER_ERROR_LOG` needs no lock either: each failure report is one append-mode
write to a validated 0600 log.

Each parallel send/receive job records its exit status in a private per-run
file. The scheduler checks all active jobs and reports missing or invalid
completion data as a failure. Where the host supports verified process-group
isolation, abort signals the complete job group; otherwise a cleanup wrapper
tracks the command's descendants. Both paths use a bounded grace period and
KILL escalation. A job that has recorded its exit status is signalled only
through its process group, never by a bare PID that may have been recycled.
Failed job or SSH control-socket cleanup makes the run fail.
The fallback wrapper cannot guarantee containment of descendants that fork
and escape its ancestry snapshots; see [architecture](./docs/architecture.md).

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
creation-order source listing entirely, and full discovery reuses the proof's
destination listing. A local destination listing checks destination existence
only when the listing itself fails.
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

When `-T` is used, destination discovery issues the same `zfs list` commands
as a local run, each over the target's ssh control master: the `name,guid`
snapshot listing, the recursive dataset inventory only when later work needs
it, the exact existence probe only after a failed listing (never after an ssh
failure, which stops the run with ssh's status 255), and the pool probe only
for a missing destination root. Each listing's own exit status decides
whether its output is used.

A `-D` progress dialog runs under the cleanup child wrapper as part of the
send pipeline: zxfer tees the stream into a private FIFO that the dialog
reads, so `-D` works with `-j`. Under `-j` the dialog lives in the job's
process group, or in its wrapper when process groups are unavailable. The
remaining local wrapper-style helpers use verified process groups where
available and a TERM-aware child wrapper otherwise.

Current runtime caveats are tracked in [KNOWN_ISSUES.md](./KNOWN_ISSUES.md).

## Testing

Run the main local validation steps:

```sh
./tests/validate.sh full
```

For a faster edit loop, `./tests/validate.sh quick` maps staged, unstaged, and
untracked paths to focused offline checks. Pass paths explicitly when needed:

```sh
./tests/validate.sh quick src/zxfer_replication.sh
./tests/run_shunit_tests.sh \
  --suite tests/test_zxfer_replication.sh --test test_name
```

Repeat `--suite ... --test ...` to select named tests across several suites;
the runner validates the complete selection before starting any of them.

Use `./tests/validate.sh --list` to see the host-risk label and purpose of each
profile. Quick mode explains its path mappings and prints relevant integration,
performance, and documentation follow-ups without running them. The dispatcher
never runs the direct host integration harness.

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
