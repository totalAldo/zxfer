zxfer
=====

`zxfer` is a POSIX shell tool for high-reliability ZFS snapshot replication
across local and remote hosts. This maintained fork focuses on safer
replication behavior, better portability, stronger failure reporting, and
faster handling of large dataset trees.

It targets FreeBSD, Linux/OpenZFS, illumos/Solaris, and current
OpenZFS-on-macOS workflows. The command is meant for production administrators,
so CLI behavior, operator-visible output, and replication semantics are treated
as public interfaces.

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
- Adaptive multi-dataset concurrency with `-j`
- Property replication, overrides, and unsupported-property skipping
- Property backup and restore with `-k` and `-e`, using hardened metadata
  storage outside dataset mountpoints
- Optional raw sends with `-w`
- Optional `zstd` compression with `-z` or a custom `zstd` compressor command
  with `-Z`
- Structured stderr failure reports with optional `ZXFER_ERROR_LOG` mirroring

## Useful Options

- `-j jobs`: run concurrent send/receive jobs; source snapshot discovery can
  use GNU `parallel` when it is available and validated
- `-V`: enable very verbose debug output and end-of-run profiling counters
- `-x pattern`: exclude datasets from recursive replication
- `-Y`: repeat replication until no sends or destroys are performed, or until
  the built-in iteration cap is reached
- `-z`: compress ssh send/receive streams with `zstd`
- `-Z "command"`: replace the default `zstd` compressor command with a custom
  variant such as `zstd -T0 -3`

See the man pages and [docs/cli-examples.md](./docs/cli-examples.md) for the
full option set and additional workflows.

## Supported Platforms

zxfer is intended to work with:

- FreeBSD with OpenZFS
- Linux with OpenZFS
- illumos / Solaris-family systems
- OpenZFS on macOS

It also supports VM-backed validation from Linux, macOS, and WSL2 hosts through
[tests/run_vm_matrix.sh](./tests/run_vm_matrix.sh).

Platform caveats, host layouts, and compatibility notes live in
[docs/platforms.md](./docs/platforms.md).

## Operational Notes

zxfer rebuilds `PATH` from a trusted allowlist and resolves required helpers to
absolute paths. Remote `zfs`, `cat`, optional GNU `parallel`, and compression
helpers are resolved per host instead of assuming the same binary path exists
everywhere.

zxfer-managed ssh connections default to `BatchMode=yes` and
`StrictHostKeyChecking=yes`. Use `ZXFER_SSH_USER_KNOWN_HOSTS_FILE` to pin an
absolute known-hosts file, or `ZXFER_SSH_USE_AMBIENT_CONFIG=1` if you need to
fall back to the ambient local ssh policy.

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

Use [tests/run_integration_zxfer.sh](./tests/run_integration_zxfer.sh)
directly only when you explicitly want the manual host-side harness on a
disposable ZFS-capable system.

Full test-layer guidance, safety notes, coverage details, and CI workflows live
in [docs/testing.md](./docs/testing.md).

## Documentation

- [docs/README.md](./docs/README.md): documentation index
- [docs/whats-new-since-v1.1.7.md](./docs/whats-new-since-v1.1.7.md): operator-focused upgrade guide from the legacy 2019 release
- [docs/platforms.md](./docs/platforms.md): platform support and compatibility notes
- [docs/testing.md](./docs/testing.md): unit, coverage, and integration workflows
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
