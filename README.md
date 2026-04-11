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

For a task-oriented option cookbook, see
[docs/cli-examples.md](./docs/cli-examples.md).

## Highlights

- POSIX `/bin/sh` implementation with no Bash dependency
- Recursive and non-recursive snapshot replication
- Local and remote replication with `-O` / `-T`
- Remote command host specs that can include wrappers such as `pfexec` or `doas`
- Optional remote compression with `zstd`
- Optional progress hooks with `-D` (uses approximate `%%size%%` values on
  remote or multi-job runs to reduce startup latency)
- Property replication, property ignore lists, and unsupported-property skipping
- Hardened property backup / restore with `-k` and `-e`, stored under a
  source-dataset-relative tree in the absolute `ZXFER_BACKUP_DIR` root with
  keyed metadata filenames per source/destination pair plus validated
  current-format header/schema markers (`#format_version`, `#source_root`,
  `#destination_root`); chained `-k` runs also forward original-source
  provenance across intermediate backups, while older or unversioned
  backup-metadata layouts remain intentionally unsupported
- Structured stderr failure reports with optional `ZXFER_ERROR_LOG` mirroring
  and opt-in command redaction
- VM-backed guest matrix with integration-by-default plus a direct file-backed integration harness

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
to allow adaptive multi-dataset source discovery and concurrent send/receive
jobs. Run the first replication in the background so both pool trees can
progress at the same time:

```sh
./zxfer -v -d -z -j8 -F -O user@host -R zroot tank/backups/ &
```

From the same host, use a custom multithreaded `zstd` command and `-Y` to
repeat replication until the destination converges while keeping the default
`-3` compression level:

```sh
./zxfer -v -d -Z 'zstd -T0 -3' -Y -j8 -F -O user@host -R tank tank/backups/
```

## Fork-Specific Options

These are some of the most visible options added or expanded in this maintained
fork. For the full CLI reference, use the man pages.

- `-j jobs`: use adaptive source snapshot discovery, optionally accelerate
  larger trees with GNU `parallel` when it is available and validated on the
  local origin side, or when the remote origin helper resolves successfully,
  and run up to that many `zfs send`/`zfs receive` jobs concurrently; missing
  helpers still fall back to the serial snapshot-discovery path, while other
  remote helper failures abort the run
- `-V`: enable very verbose debug output and emit end-of-run profiling counters
  and accumulated stage timings to stderr, including ssh control-socket and
  remote-capability cache wait contention, capability-bootstrap source totals,
  remaining direct remote helper probes, plus prefixed live remote-command,
  remote-probe, and ssh control-socket check/open lines before each ssh
  execution
- `-w`: use raw `zfs send`
- `-x pattern`: exclude datasets whose names match a regex from recursive
  replication; use `-x '^tank/data$'` to exclude only `tank/data`
- `-Y`: repeat replication until no sends or destroys are performed, or until
  the built-in 8-iteration cap is reached
- `-z`: compress ssh send/receive streams with `zstd`; remote source
  snapshot-list metadata discovery uses the same compression/decompression
  pipeline on `-O ... -j ...` discovery runs
- `-Z "command"`: replace the default compression command with a custom `zstd`
  pipeline, for example multithreaded `zstd -T0 -3`. Higher levels such as
  `-9` trade much more CPU time for smaller ratio gains and are not the
  default.

## Performance And Maintainability Improvements

Compared with the older upstream base, the current fork includes:

- Adaptive source snapshot discovery plus concurrent send/receive execution
  when `-j` is used
- `zfs send -I` incremental replication so the full snapshot chain is sent in
  one stream when appropriate
- Exact dataset and snapshot diffing with `comm`, which avoids older
  nested-loop comparison paths and scales much better on large snapshot sets
- Destination-side snapshot discovery that only inspects the intended dataset
  and only lists snapshot names, avoiding unnecessary metadata sorting work
- Batched destination snapshot deletion plus background destroy handling so
  cleanup can proceed efficiently during replication
- Batched per-dataset `zfs set` property updates during reconciliation, cutting
  remote `ssh` round trips on `-P`, `-o`, and post-seed property passes
- SSH control-socket reuse within a zxfer run for `-O` and `-T`, reducing
  repeated connection setup overhead without sharing temp roots across
  separate zxfer invocations
- Optional `zstd` compression for ssh replication plus customizable `-Z`
  compression commands, with remote source snapshot-list discovery reusing the
  same validated compression pipeline
- Requested-tool-aware remote capability handshakes and per-run cache reuse,
  so startup preloads only the minimum remote `zfs`/OS state while later
  `parallel`, `cat`, and `zstd`-style helper lookups can still reuse scoped
  cached probe results inside the same run instead of opening separate ad hoc
  ssh helper queries
- Deterministic snapshot sorting and comparison via `LC_ALL=C`, so snapshot
  planning behaves consistently across Linux, FreeBSD, macOS, and Solaris-like
  environments
- Secure-PATH resolution for required helpers, plus optional remote GNU
  `parallel` acceleration, so mixed-platform hosts do not depend on matching
  binary locations and still fall back safely when the fast discovery helper is
  unavailable
- Structured failure reporting, optional `ZXFER_ERROR_LOG` mirroring, and much
  broader shunit2 and integration coverage

## Code Refactoring

The current tree has also been reworked for readability and maintainability:

- `zxfer` now sources only `src/zxfer_modules.sh`, which centralizes module
  source order for the launcher, tests, and direct-sourcing fixtures
- `src/` remains flat, with focused modules grouped by long-lived
  responsibilities such as reporting, exec, runtime, CLI, snapshot state,
  remote hosts, property reconciliation, and replication
- startup state now flows through explicit init/reset helpers rather than
  relying on source-time scratch-global initialization
- module names stay purpose-based; broad reuse is not, by itself, a reason to
  introduce generic files such as `common`, `globals`, `utils`, or `lib`
- helper functions are smaller and more testable than the older monolithic flow
- quoting, ssh, backup-metadata, and failure-reporting paths are centralized
- the test suite now covers shell helpers, snapshot discovery, property logic,
  send/receive plumbing, and the file-backed integration harness

## Documentation Map

- [docs/README.md](./docs/README.md): documentation index
- [docs/platforms.md](./docs/platforms.md): platform support and compatibility notes
- [docs/external-tools.md](./docs/external-tools.md): external tool inventory for packaging, ports, and dependency review
- [docs/cli-examples.md](./docs/cli-examples.md): consolidated CLI examples for every current option
- [docs/testing.md](./docs/testing.md): unit, coverage, and integration workflows
- [docs/coding-style.md](./docs/coding-style.md): project-specific shell, module, and test style guide
- [docs/troubleshooting.md](./docs/troubleshooting.md): common failures and what they usually mean
- [docs/architecture.md](./docs/architecture.md): module layout and replication flow
- [docs/roadmap.md](./docs/roadmap.md): planned feature work, compatibility-floor direction, and remaining refactors
- [docs/upstream-history.md](./docs/upstream-history.md): historical context and removed legacy behavior
- [examples/README.md](./examples/README.md): runnable command templates for common replication flows, including the `ZXFER_ERROR_LOG` mail wrapper and its multi-source `SRC_DATASETS` mode
- [KNOWN_ISSUES.md](./KNOWN_ISSUES.md): current open issues
- [CHANGELOG.txt](./CHANGELOG.txt): release history
- [SECURITY.md](./SECURITY.md): security model and reporting guidance
- [CONTRIBUTING.md](./CONTRIBUTING.md): contributor workflow

## Platform Notes

Primary development and current manual testing have been on FreeBSD 14.x and
FreeBSD 15.x, but this fork also supports:

- Linux with OpenZFS
- illumos/Solaris systems with `zfs` / `svcadm`
- OpenZFS on macOS, including `/usr/local/zfs/bin` layouts, with platform-specific testing and certification notes documented in `docs/testing.md` and `docs/platforms.md`
- VM-backed integration orchestration hosts on Linux, macOS, and Windows via
  WSL2, with native Windows shell orchestration intentionally out of scope

zxfer resolves `zfs`, `ssh`, `awk`, and other required tools through a trusted
secure-PATH model. Remote `zfs`, `cat`, optional GNU `parallel`, and
compression helper lookups are resolved on the remote host rather than
assuming the same absolute path exists everywhere. When the local origin-side
`parallel` helper is missing or is not GNU `parallel`, zxfer keeps `-j`
send/receive concurrency but falls back to the serial source snapshot-
discovery path. On remote-origin runs, a missing origin-host helper still
falls back to serial discovery, but other remote helper probe or execution
failures stop the run instead of being masked as a fallback.
Custom `-Z` compression commands now flow through the same validated helper-
resolution path for both replication streams and remote source snapshot
discovery. zxfer-managed ssh connections default to
`BatchMode=yes` plus `StrictHostKeyChecking=yes`; set
`ZXFER_SSH_USER_KNOWN_HOSTS_FILE` to pin a specific absolute known-hosts file,
or set `ZXFER_SSH_USE_AMBIENT_CONFIG=1` to fall back to the ambient local ssh
trust policy.

Current open runtime caveats are tracked in [KNOWN_ISSUES.md](./KNOWN_ISSUES.md). Testing workflow guidance lives in [docs/testing.md](./docs/testing.md).

## Testing

Run all shunit2 suites with the default bounded parallel worker count:

```sh
./tests/run_shunit_tests.sh
```

Force serial shunit2 execution:

```sh
./tests/run_shunit_tests.sh --jobs 1
```

`--jobs 1` keeps the historical foreground behavior and streams suite output as
it is generated.

If you request more jobs than runnable suites, the runner prints a notice and
clamps `--jobs` to the suite count.

Run local lint with the same pinned toolchain as CI:

```sh
./tests/run_lint.sh
```

For a ready-made contributor environment, open the repository in the included
VS Code / GitHub Codespaces devcontainer. It preinstalls `dash`,
`bash-posix`, `busybox-ash`, `posh`, `kcov`, Ubuntu `zfsutils-linux`
userland (`zfs`, `zpool`), and the pinned lint toolchain used by
`tests/run_lint.sh`, but it does not replace a real ZFS-capable host or the
preferred VM-backed integration path.

Run shell coverage:

```sh
./tests/run_coverage.sh
```

Run the enforced bash-xtrace coverage gate locally:

```sh
ZXFER_COVERAGE_MODE=bash-xtrace ./tests/run_coverage.sh
```

Run the unattended VM-backed smoke profile:

```sh
./tests/run_vm_matrix.sh --profile smoke
```

Run the default local VM-backed profile:

```sh
./tests/run_vm_matrix.sh --profile local
```

List the supported profiles or guest names before choosing a narrower run:

```sh
./tests/run_vm_matrix.sh --list-profiles
./tests/run_vm_matrix.sh --list-guests
```

Run the shunit2 suites inside the selected guests without changing the default
integration path:

```sh
./tests/run_vm_matrix.sh --profile local --test-layer shunit2
```

Run the local profile with live guest stdout/stderr mirrored to the console:

```sh
./tests/run_vm_matrix.sh --profile local --stream-guest-output
```

Run the local profile in an AI-friendly failure-only mode:

```sh
./tests/run_vm_matrix.sh --profile local --failed-tests-only
```

Cherry-pick one or more named integration tests inside the guest:

```sh
./tests/run_vm_matrix.sh --profile local --guest ubuntu --only-test basic_replication_test,force_rollback_test
```

The runner also logs host-side setup phases so local runs do not look idle
while it refreshes checksum manifests, reuses or downloads guest images,
prepares base images, and waits for guest SSH readiness. Interactive serial
downloads show a curl progress bar automatically.
FreeBSD local guests now boot with an attached `cidata` config-drive because
the official BASIC-CLOUDINIT images expect `nuageinit` seed media rather than
the Ubuntu-style `nocloud-net` SMBIOS path. OmniOS local guests now wait for
their first-boot SSH host key to stop changing before guest preparation and
the selected test layer begin.

Run up to two selected guests in parallel:

```sh
./tests/run_vm_matrix.sh --profile local --jobs 2
```

If you need to stop a local run, press `Ctrl+C`. The runner signals active
guest workers, waits for backend cleanup, and then exits non-zero.

Run the full VM-backed matrix and keep failed guest state for debugging:

```sh
./tests/run_vm_matrix.sh --profile full --preserve-failed-guests
```

Run the direct-host integration harness manually when you specifically want an
interactive host-side run on a disposable ZFS-capable system:

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

Continue after failures but only replay failing test output:

```sh
./tests/run_integration_zxfer.sh --yes --keep-going --failed-tests-only
```

Run only specific named integration tests:

```sh
./tests/run_integration_zxfer.sh --yes --only-test basic_replication_test,force_rollback_test
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
- shunit2 workflow on Ubuntu and macOS, plus an Ubuntu portable-shell matrix
  for `dash`, `bash --posix`, and `busybox ash`; the `posh` lane is currently
  disabled because its execution time exceeds 30 minutes on GitHub-hosted
  runners; plus additional FreeBSD and OmniOS VM-backed unit jobs
- ZFS integration workflow using the direct-host Linux harness on
  `ubuntu-24.04`, plus FreeBSD and OmniOS guest-local `vmactions` lanes that
  install their native prerequisites and run `tests/run_integration_zxfer.sh`
  directly inside the guest, with preserved failure artifacts uploaded for
  each platform

Hosted macOS CI is currently used for unit and shell-portability coverage, not
as a required ZFS integration gate, because Darwin/OpenZFS property behavior
is not yet stable enough for strict end-to-end certification across the same
property assertions used on FreeBSD and Linux.
Local macOS and WSL2 VM-matrix runs are supported through QEMU. Apple Silicon
macOS hosts now prefer official arm64 Ubuntu and FreeBSD guests where those
images exist, so the `smoke` and `local` profiles can use hardware-
virtualized ARM guests instead of x86_64 emulation when QEMU's aarch64 UEFI
firmware is installed. OmniOS remains an x86_64 guest today, so `full` runs on
Apple Silicon still fall back to slower best-effort TCG emulation for that
lane. Those TCG runs are useful for coverage and debugging, but they are not
the strict isolation gate.
The VM runner defaults to serial guest execution and writes per-guest
stdout/stderr logs under the configured artifact root. Add
`--stream-guest-output` to mirror those guest logs live, or `--jobs N` to run
multiple selected guests in parallel when the host has enough CPU and memory
headroom. The default guest test layer is `integration`; add
`--test-layer shunit2` when you want the same guest boundary to run
`tests/run_shunit_tests.sh` instead. `--failed-tests-only` and
`--only-test name[,name...]` remain integration-only filters. Add
`--list-profiles` or `--list-guests` when you want the runner to print the
current supported selections without starting a guest. The default
`--backend auto` mode resolves to local `qemu` unless
`ZXFER_VM_CI_MANAGED_GUEST` pins a single guest for the `ci-managed` backend.
Add
`--failed-tests-only` when you want the in-guest integration harness to
suppress passing test chatter and only replay failing test output plus the
final summary; that mode now also implies live guest output streaming so each
completed non-failing test still prints a compact `[N/TOTAL] PASS test_name`
or `[N/TOTAL] SKIP test_name` line, and failures replay the full captured
stdout/stderr for that specific test with labeled output sections. Add
`--only-test name[,name...]` when you want a tighter guest-backed integration
loop around just the affected cases instead of rerunning the full selected
profile on every iteration.
FreeBSD and OmniOS now also have hosted GitHub Actions lanes for both unit and
integration coverage through VM-backed jobs, but current project testing still
includes local validation on FreeBSD 14.x and 15.x in addition to those hosted
checks.
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
