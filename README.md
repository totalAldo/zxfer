zxfer
=====

`zxfer` is a POSIX shell tool for ZFS snapshot replication across local and
remote hosts. This maintained fork refactored the older upstream utility to
improve replication performance, code readability, and operational safety on
large dataset trees.

The project’s modifications were motivated by the need to reduce both `ssh` and
local replication time while improving code readability and maintainability.
That work added stronger error handling, new options, and performance-oriented
changes, and it now focuses on high-reliability `zfs send` / `zfs receive`
replication, safer failure handling, stronger dependency resolution, and better
test coverage across FreeBSD, Linux, illumos/Solaris, and OpenZFS on macOS.

For the full CLI reference, use the man page:

```sh
man zxfer
```

Or read the bundled manpages in this checkout:

- [man/zxfer.8](./man/zxfer.8) for FreeBSD/Linux-style installs
- [man/zxfer.1m](./man/zxfer.1m) for Solaris/illumos-style installs

## Highlights

- POSIX `/bin/sh` implementation with no Bash dependency
- Recursive and non-recursive snapshot replication
- Local and remote replication with `-O` / `-T`
- Remote host specs that can include wrappers such as `pfexec` or `doas`
- Optional remote compression with `zstd`
- Optional progress hooks with `-D`
- Property replication, property ignore lists, and unsupported-property skipping
- Hardened property backup / restore with `-k` and `-e`
- Structured stderr failure reports with optional `ZXFER_ERROR_LOG` mirroring
- File-backed integration harness plus shunit2 unit coverage

## Quick Start

Replicate a local recursive dataset tree:

```sh
./zxfer -v -R tank/data backup/data
```

Pull snapshots from a remote host:

```sh
./zxfer -v -O user@example.com -R zroot backup/zroot
```

Repeat until there are no remaining changes:

```sh
./zxfer -v -Y -R tank/src backup/dst
```

Use remote compression:

```sh
./zxfer -v -z -T backup@example.com -R tank/src backup/dst
```

## Performance-Oriented Examples

Replicate two remote pools from the same origin host over `ssh`, using `-j8`
to parallelize source snapshot discovery and allow concurrent send/receive
jobs. Run the first replication in the background so both pool trees can
progress at the same time:

```sh
./zxfer -v -d -z -j8 -F -O user@host -R zroot tank/backups/ &
```

From the same host, use a custom `zstd` command and `-Y` to repeat replication
until the destination converges:

```sh
./zxfer -v -d -Z 'zstd -T0 -9' -Y -j8 -F -O user@host -R tank tank/backups/
```

## Fork-Specific Options

These are some of the most visible options added or expanded in this maintained
fork. For the full CLI reference, use the man pages.

- `-j jobs`: parallelize source snapshot discovery with GNU `parallel` and run
  up to that many `zfs send`/`zfs receive` jobs concurrently
- `-V`: enable very verbose debug output and emit end-of-run profiling counters
  to stderr
- `-w`: use raw `zfs send`
- `-x pattern`: exclude datasets whose names match a regex from recursive
  replication; use `-x '^tank/data$'` to exclude only `tank/data`
- `-Y`: repeat replication until no sends or destroys are performed, or until
  the built-in 8-iteration cap is reached
- `-z`: compress ssh transfers with `zstd`
- `-Z "command"`: replace the default compression command with a custom `zstd`
  pipeline, for example multithreaded or higher-compression settings

## Performance And Maintainability Improvements

Compared with the older upstream base, the current fork includes:

- Parallel source snapshot discovery and concurrent send/receive execution when
  `-j` is used
- `zfs send -I` incremental replication so the full snapshot chain is sent in
  one stream when appropriate
- Exact dataset and snapshot diffing with `comm`, which avoids older
  nested-loop comparison paths and scales much better on large snapshot sets
- Destination-side snapshot discovery that only inspects the intended dataset
  and only lists snapshot names, avoiding unnecessary metadata sorting work
- Batched destination snapshot deletion plus background destroy handling so
  cleanup can proceed efficiently during replication
- SSH control-socket reuse for `-O` and `-T`, reducing repeated connection
  setup overhead
- Optional `zstd` compression for ssh replication plus customizable `-Z`
  compression commands
- Deterministic snapshot sorting and comparison via `LC_ALL=C`, so snapshot
  planning behaves consistently across Linux, FreeBSD, macOS, and Solaris-like
  environments
- Secure-PATH resolution for required helpers, including remote `zfs`, `cat`,
  and GNU `parallel`, so mixed-platform hosts do not depend on matching binary
  locations
- Structured failure reporting, optional `ZXFER_ERROR_LOG` mirroring, and much
  broader shunit2 and integration coverage

## Code Refactoring

The current tree has also been reworked for readability and maintainability:

- functionality is split into focused shell modules under `src/`
- helper functions are smaller and more testable than the older monolithic flow
- quoting, ssh, backup-metadata, and failure-reporting paths are centralized
- the test suite now covers shell helpers, snapshot discovery, property logic,
  send/receive plumbing, and the file-backed integration harness

## Documentation Map

- [docs/README.md](./docs/README.md): documentation index
- [docs/platforms.md](./docs/platforms.md): platform support and compatibility notes
- [docs/testing.md](./docs/testing.md): unit, coverage, and integration workflows
- [docs/troubleshooting.md](./docs/troubleshooting.md): common failures and what they usually mean
- [docs/architecture.md](./docs/architecture.md): module layout and replication flow
- [docs/upstream-history.md](./docs/upstream-history.md): historical context and removed legacy behavior
- [examples/README.md](./examples/README.md): runnable command templates for common replication flows, including the `ZXFER_ERROR_LOG` mail wrapper and its multi-source `SRC_DATASETS` mode
- [KNOWN_ISSUES.md](./KNOWN_ISSUES.md): current open issues and testing limitations
- [CHANGELOG.txt](./CHANGELOG.txt): release history
- [SECURITY.md](./SECURITY.md): security model and reporting guidance
- [CONTRIBUTING.md](./CONTRIBUTING.md): contributor workflow

## Platform Notes

Primary development and current manual testing have been on FreeBSD 14.x and
FreeBSD 15.x, but this fork also supports:

- Linux with OpenZFS
- illumos/Solaris systems with `zfs` / `svcadm`
- OpenZFS on macOS, including `/usr/local/zfs/bin` layouts, with platform-specific property caveats tracked in `KNOWN_ISSUES.md`

zxfer resolves `zfs`, `ssh`, `awk`, and other required tools through a trusted
secure-PATH model. Remote `zfs`, `cat`, and GNU `parallel` lookups are resolved
on the remote host rather than assuming the same absolute path exists
everywhere. Remote `uname` and source-listing `zstd` resolution still have open
hardening gaps; see [KNOWN_ISSUES.md](./KNOWN_ISSUES.md).

Current caveats are tracked in [KNOWN_ISSUES.md](./KNOWN_ISSUES.md).

## Testing

Run all shunit2 suites:

```sh
./tests/run_shunit_tests.sh
```

Run local lint with the same pinned toolchain as CI:

```sh
./tests/run_lint.sh
```

Run shell coverage:

```sh
./tests/run_coverage.sh
```

Run the enforced bash-xtrace coverage gate locally:

```sh
ZXFER_COVERAGE_MODE=bash-xtrace ./tests/run_coverage.sh
```

Run the integration harness:

```sh
./tests/run_integration_zxfer.sh
```

Run the integration harness unattended:

```sh
./tests/run_integration_zxfer.sh --yes
```

Continue after failures and print a summary:

```sh
./tests/run_integration_zxfer.sh --yes --keep-going
```

GitHub Actions includes:

- lint workflow, via the shared `tests/run_lint.sh` bootstrap, including
  pinned `actionlint`, `checkbashisms`, `shfmt`, `codespell`, and ShellCheck
  toolchains
- shell coverage workflow with both the shipped `bash-xtrace` fallback and a
  Docker-backed `kcov` pass, each uploaded as its own artifact; the
  bash-xtrace lane enforces committed per-file and total coverage minimums,
  rejects regressions versus the checked-in baseline, and publishes the
  `missing.txt` diff in the GitHub step summary
- shunit2 workflow on `ubuntu-latest` and `macos-latest`, plus an Ubuntu
  portable-shell matrix for `dash`, `bash --posix`, `busybox ash`, and an
  initially non-blocking `posh` lane
- Ubuntu ZFS integration workflow using file-backed test pools and
  `--keep-going` failure collection, with failed workdirs uploaded as artifacts

Hosted macOS CI is currently used for unit and shell-portability coverage, not
as a required ZFS integration gate, because the Darwin/OpenZFS property
behavior described in `KNOWN_ISSUES.md` is not yet stable enough for strict
end-to-end certification.
FreeBSD coverage is currently maintained through local/manual validation rather
than a hosted GitHub Actions lane, and current project testing includes
FreeBSD 15.x in addition to the long-running FreeBSD 14.x environment.
The coverage workflow keeps the shipped bash-xtrace fallback and also runs a
separate Linux `kcov` pass, which gives better visibility into child-shell and
launcher coverage without depending on a runner-installed `kcov` package or an
unpinned container tag. The enforced baseline lives in
`tests/coverage_baseline/bash-xtrace/`, and the explicit floor policy lives in
`tests/coverage_policy.tsv`.

See [docs/testing.md](./docs/testing.md) for full details and safety notes.

## Project Status

- Active fork focused on reliability, portability, and testability
- Legacy rsync mode (`-S`) has been removed
- Known open issues are tracked in [KNOWN_ISSUES.md](./KNOWN_ISSUES.md)

## Feedback

Issues and pull requests are welcome. For contribution guidelines, see
[CONTRIBUTING.md](./CONTRIBUTING.md).

## Acknowledgements

A big thank you to everyone who has contributed to zxfer over the years, and to
the operators who have continued using and testing it across multiple ZFS
platforms.
