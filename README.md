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
  supervised long-lived cleanup via `-j`
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
- Metadata-bearing lock and lease coordination for ssh control sockets, remote
  capability caches, and `ZXFER_ERROR_LOG` appends, with validated stale-owner
  reaping and checked release semantics

## Useful Options

- `-j jobs`: run concurrent send/receive jobs; when `jobs > 1`, zxfer uses
  explicit per-dataset source discovery instead of the serial recursive
  listing. Source discovery runs as a tracked background helper with staged
  stderr and registered PID cleanup, while long-lived send/receive workers run
  under a shared supervisor that records launch/completion metadata and aborts
  through validated process-group or owned-child-set cleanup instead of
  signaling a bare wrapper PID. zxfer also serializes conflicting ancestor/descendant
  destination receives on the same target, even when spare job slots remain,
  so parent and child datasets do not receive concurrently. Local-origin and
  remote-origin runs require a resolved `parallel` helper on the executing
  origin host; zxfer intentionally checks only that the helper exists through
  the secure-PATH model, so operators and packages must provide an
  implementation compatible with the GNU Parallel-style options used by the
  rendered source-discovery pipeline
- `-V`: enable very verbose debug output and end-of-run profiling counters,
  including startup latency and trap-cleanup timing
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

- FreeBSD 14.x and 15.x maintained branches with OpenZFS
- Linux with OpenZFS
- current OmniOS / illumos systems
- current OpenZFS on macOS workflows

For releases published after 2026-05-01, zxfer follows maintained FreeBSD
branches and does not guarantee support for FreeBSD 13.x or other
end-of-life FreeBSD branches. Pre-OpenZFS 2.0 behavior, Solaris Express-era
property profiles, and older backup metadata layouts are intentionally
unsupported.

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

Shared ssh control sockets, remote capability caches, and `ZXFER_ERROR_LOG`
appends are coordinated through metadata-bearing lock or lease directories that
record owner PID, process-start identity, hostname, purpose, and creation
time. The current native format is directory-based rather than plain pid files:
ssh `.lock` paths, ssh `leases/lease.*`, remote capability `<cache>.lock`, and
`ZXFER_ERROR_LOG` lock paths all carry validated owner metadata. zxfer
validates and reaps stale or corrupt owners before reuse, checks release
operations instead of silently suppressing failures, and warns during trap
cleanup if a registered owned lock or lease cannot be released while
preserving the original zxfer exit status. Pre-metadata cache artifacts from
older releases are no longer supported: if a reused cache root still contains
plain ssh `leases/lease.*` files or pid-only `.lock` directories, remove the
stale entry or cache root before rerunning zxfer.

Long-lived parallel send/receive work runs under private supervisor control
directories beneath the runtime temp root. Each job records launch and
completion metadata, and trap-time abort validates the recorded process group
or owned child set before signaling it. Abort cleanup is completion-aware: if
`completion.tsv` is already present, or a failed signal is followed by a
refreshed process snapshot that no longer shows the runner, zxfer treats the
job as already finished and continues cleanup. It fails closed only when a
refreshed snapshot still shows a live owned runner that cannot be validated or
signaled, or when the completion record itself cannot be persisted. The same
checked-cleanup rule now applies to ssh control-socket teardown during trap
cleanup: if zxfer cannot close a managed socket after otherwise successful
work, it exits nonzero instead of reporting a clean run.

For `-j` send/receive work, the scheduler also treats ancestor/descendant
destination datasets on the same target as mutually exclusive. zxfer now waits
for the conflicting receive to finish before launching the next transfer, so
recursive parent/child destination trees no longer race each other and degrade
later into truncated-stream collateral failures.

Short-lived local background helpers that still need shell wrappers, such as
progress dialogs and delete-planning identity writers, now publish validated
cleanup metadata through the shared runtime registry. The remaining local
wrapper-style helpers run under a small TERM-aware child wrapper so early-exit
cleanup no longer falls back to signaling a bare wrapper-shell PID.

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
