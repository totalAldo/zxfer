# External Tool Inventory

This document is the maintained inventory of external tools used by `zxfer`.
It is intended to help with packaging work, especially FreeBSD ports
dependency review.

The list is split into:

- installed-command runtime requirements
- optional runtime feature tools
- base userland utilities assumed to exist on supported systems
- test, coverage, integration, and CI tooling

For FreeBSD ports work, the important distinction is that many tools below come
from the base system and should not become `RUN_DEPENDS`. Only tools that are
not in base, or that you want guaranteed for optional features, should be added
as package dependencies.

## Installed `zxfer` Runtime

### Hard Runtime Requirements

These tools are required by the installed `zxfer` command itself.

| Tool | Why it is needed | Resolution path | FreeBSD ports note |
| --- | --- | --- | --- |
| `/bin/sh` | interpreter for `zxfer` and `src/*.sh` | script shebang | base system |
| `zfs` | all replication, property, snapshot, and existence operations | resolved through the secure-PATH model locally; resolved per host remotely | base system on supported FreeBSD/OpenZFS installs |
| `awk` | parsing, normalization, sorting helpers, report rendering, and cache/index helpers | resolved through the secure-PATH model locally | base system |
| `ps` | supervised background-job identity validation, process-group checks, and owned-child-set teardown discovery | resolved through the secure-PATH model locally | base system |

Notes:

- On SunOS/illumos, `gawk` is preferred when available, but plain `awk`
  remains the baseline dependency.

### Conditional Runtime Requirements

These tools are only required when the corresponding feature is used.

| Tool | Used by | When required | Packaging guidance |
| --- | --- | --- | --- |
| `ssh` | remote host probing, remote command execution, control sockets | required when `-O` or `-T` is used, and resolved lazily through the secure-PATH model when remote transport is actually needed | base system on the supported host families; do not make it a local-only hard dependency |
| `cat` | property backup restore and remote backup metadata writes | `-e` restore mode on the origin side, plus `-k` when backup metadata is written through a remote target helper; remote `cat` is resolved per host role | base system; do not add a separate FreeBSD package dependency |
| `parallel` | explicit per-dataset source snapshot discovery for `-j > 1` | required on the executing origin host whenever `-j > 1` is requested; zxfer intentionally validates only that a helper named `parallel` resolves through the secure-PATH model, then assumes the operator/package supplied a compatible implementation; the rendered pipeline uses GNU Parallel-style options and does not silently fall back to the serial recursive listing once `-j > 1` is requested | consider a package dependency if the port should guarantee `-j > 1` support out of the box |
| `setsid` | supervisor runner process-group isolation | optional; when the local host provides `setsid`, the background-job runner prefers launching long-lived workers in a dedicated process group and falls back to owned-child-set teardown otherwise | usually a base or util-linux userland tool; do not make it a hard dependency unless packaging wants to require process-group isolation everywhere |
| `zstd` | compressed send/receive streams and remote snapshot-discovery metadata compression | `-z` or default/custom `-Z` compression paths, including remote `-O ... -j ...` metadata discovery when ssh compression is active | consider a package dependency only if the port should guarantee compression support out of the box |
| `svcadm` | migration/service handling | `-c` and `-m` on OmniOS/illumos systems | not a FreeBSD package dependency |
| `kldstat`, `kldload`, `/dev/speaker` | audible status beeps | FreeBSD-only `-b` / `-B` path | base system and device availability; not a package dependency |

### Operator-Supplied Wrapper Commands

These are not dependencies of `zxfer` itself, but users may include them in
`-O` / `-T` host specs:

- `pfexec`
- `doas`
- similar privilege wrappers such as `sudo`

Those commands should not be treated as unconditional package dependencies.
They are environment-specific operator choices.

## Base Userland Utilities Used At Runtime

The installed command also invokes standard userland utilities that are
normally present on supported systems.

Current runtime inventory:

- `cat`
- `chmod`
- `cksum`
- `comm`
- `cut`
- `date`
- `find`
- `grep`
- `head`
- `hostname`
- `id`
- `kill`
- `ls`
- `mkdir`
- `mkfifo`
- `mktemp`
- `mv`
- `od`
- `printf`
- `ps`
- `rm`
- `sed`
- `sort`
- `stat`
- `tail`
- `tr`
- `uname`
- `wc`

On FreeBSD, these are expected from base and usually do not belong in
`RUN_DEPENDS`.

## Direct Integration Harness Dependencies

These tools are used by [run_integration_zxfer.sh](../tests/run_integration_zxfer.sh),
not by the installed `zxfer` command.

| Tool | Why it is needed |
| --- | --- |
| `zfs` | create datasets, snapshots, properties, and assertions |
| `zpool` | create and destroy file-backed test pools |
| `mktemp`, `mkdir`, `chmod`, `rm`, `ln`, `kill`, `chown` | harness workdir, wrappers, and cleanup |
| `truncate` or `mkfile` or `perl` or `python3` | safe sparse-file creation for test vdevs |
| `parallel` | integration cases that exercise `-j` behavior |
| `zstd` | integration cases that exercise compressed replication |
| `bash` | CI convenience dependency for some hosted test lanes and wrappers |

Important packaging note:

- `zpool` is a test-harness dependency, not an installed-command runtime
  dependency.

## Manual Performance Harness Dependencies

These tools are used by [run_perf_tests.sh](../tests/run_perf_tests.sh), not by
the installed `zxfer` command. The performance harness sources the direct
integration harness in source-only mode, so it inherits the direct integration
harness dependencies above.

| Tool | Why it is needed |
| --- | --- |
| `dd` | write deterministic payload files into the temporary test datasets |
| `awk`, `date` | summarize timing samples, throughput, and baseline comparisons |
| mock `ssh` and passthrough `zstd` helpers | exercise remote and compressed remote cases without leaving the temporary host or guest |

Perf dependencies are local QA dependencies only. They should not become
installed-command runtime dependencies.

## VM Matrix Host Dependencies

These tools are used by [run_vm_matrix.sh](../tests/run_vm_matrix.sh) on the
host that orchestrates disposable guests:

| Tool | Why it is needed |
| --- | --- |
| `qemu-system-x86_64` | boot x86_64 guests for the local `qemu` backend |
| `qemu-system-aarch64` | boot preferred arm64 guests on Apple Silicon and other arm64 hosts |
| `edk2-aarch64-code.fd` or equivalent QEMU UEFI firmware | boot arm64 cloud images under the local `qemu` backend |
| `qemu-img` | create writable overlays above cached base images |
| `curl` | download guest images and checksum manifests |
| `python3` | serve NoCloud metadata and allocate local ephemeral TCP ports |
| `ssh`, `ssh-keygen`, `ssh-keyscan` | provision guests, copy the checkout, and collect artifacts |
| `tar` | transfer the current checkout into guests and stream guest artifacts back out |
| `git` | validate and archive `ZXFER_VM_PERF_BASELINE_REF` for the optional `perf-compare` guest layer |
| `xz` | decompress the pinned FreeBSD cloud image |
| `zstd` | future-proof `.zst` guest image support when a selected guest ships one |

These are local QA or CI-orchestration dependencies, not installed-command
runtime dependencies of `zxfer`.

## Unit, Coverage, And Lint Tooling

These tools are used for development, CI, or local QA.

### Unit Test Runner

- `tests/shunit2/shunit2` is vendored in the repository
- `/bin/sh` is sufficient for the normal shunit2 runner
- alternate shells such as `dash`, `bash --posix`, `busybox ash`, and
  `/usr/xpg4/bin/sh` are CI/test-matrix tools, not runtime dependencies
- OmniOS shunit2 guest runs through `tests/run_vm_matrix.sh --test-layer shunit2`
  install `bash` in the guest and export a `bash --posix` wrapper via
  `ZXFER_TEST_SHELL`, because `/usr/xpg4/bin/sh` does not honor the mock-heavy
  subshell helper overrides the same way

### Coverage Tooling

| Tool | Why it is needed |
| --- | --- |
| `bash` | required for `ZXFER_COVERAGE_MODE=bash-xtrace` |
| `kcov` | optional higher-fidelity coverage path |
| `docker` | CI-only `kcov` lane on GitHub Actions |

### Lint Bootstrap Tooling

The lint bootstrap script [run_lint.sh](../tests/run_lint.sh) requires:

- `curl`
- `git`
- `tar`
- `ar`
- `perl`
- `python3`
- `sha256sum` or `shasum`

It then bootstraps and runs these pinned tools:

- `actionlint`
- `checkbashisms`
- `shfmt`
- `codespell`
- `shellcheck`

These are developer or CI dependencies, not runtime dependencies of the
installed command.

### VS Code / Codespaces Devcontainer

The repository also ships `.devcontainer/` for GitHub Codespaces and local
VS Code container use. That image is based on Ubuntu 24.04 and preinstalls:

- `dash`
- `bash-posix`
- `busybox-ash`
- `posh`
- Ubuntu `zfsutils-linux` userland (`zfs`, `zpool`)
- the pinned lint-tool cache used by `tests/run_lint.sh`
- upstream `kcov`, built from a pinned release because Ubuntu 24.04 does not
  currently ship a native `kcov` package

This is a contributor convenience environment for lint, unit, and local
coverage work. It is not an installed-command runtime dependency and does not
provide a live ZFS kernel stack, so it still does not replace a real
ZFS-capable host or VM for the direct integration harness or the VM matrix.

## Packaging Guidance For FreeBSD Ports

For a minimal FreeBSD port of the installed command:

- treat `/bin/sh`, `zfs`, `ssh`, `awk`, and the standard BSD userland tools as
  base-system assumptions
- do not add `zpool` as a runtime dependency; it is only needed by the
  integration harness

If the port should guarantee optional features out of the box, consider:

- `parallel` for `-j`
- `zstd` for `-z` and default/custom `-Z`

If the port runs extended QA beyond basic install/package checks, consider
separate `TEST_DEPENDS` or maintainer tooling for:

- `bash`
- `kcov`
- lint bootstrap requirements

## Maintenance Rule

When code changes add, remove, or retarget external tools, update this file in
the same change, and review related packaging or CI artifacts for drift.
