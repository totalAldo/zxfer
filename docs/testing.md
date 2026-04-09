# Testing

## Test Layers

The project currently uses three practical layers of validation:

- shunit2 unit tests
- shell coverage reporting
- file-backed ZFS integration tests

## Unit Tests

Run all suites:

```sh
./tests/run_shunit_tests.sh
```

Run the local lint stack with the same pinned toolchain as CI:

```sh
./tests/run_lint.sh
```

For a ready-made contributor environment, open the repository in the included
VS Code / GitHub Codespaces devcontainer. It tracks the Ubuntu 24.04 CI host
family closely enough for local lint, shunit2, and coverage work, and it
preinstalls:

- `dash`
- `bash-posix`
- `busybox-ash`
- `posh`
- `kcov`
- Ubuntu `zfsutils-linux` userland (`zfs`, `zpool`)
- the pinned `actionlint`, `checkbashisms`, `shfmt`, `codespell`, and
  ShellCheck toolchain from `tests/run_lint.sh`

The devcontainer is still not a substitute for a real ZFS-capable host. Use
it for shell-portability work, linting, and local `kcov` runs such as:

```sh
ZXFER_COVERAGE_MODE=kcov ./tests/run_coverage.sh
```

Keep `tests/run_integration_zxfer.sh` on a disposable VM or other safe system
that can create and destroy file-backed zpools.

Run one suite:

```sh
./tests/run_shunit_tests.sh tests/test_zxfer_replication.sh
```

Run the suites under a specific alternate shell:

```sh
ZXFER_TEST_SHELL=/bin/dash ./tests/run_shunit_tests.sh
```

For multi-word shell modes such as `bash --posix`, point `ZXFER_TEST_SHELL` at
an executable wrapper script that `exec`s the desired command.

The test layout broadly follows the source layout:

- `test_run_shunit_tests.sh`
- `test_zxfer_reporting.sh`
- `test_zxfer_exec.sh`
- `test_zxfer_dependencies.sh`
- `test_zxfer_runtime.sh`
- `test_zxfer_cli.sh`
- `test_zxfer_snapshot_state.sh`
- `test_zxfer_backup_metadata.sh`
- `test_zxfer_remote_hosts.sh`
- `test_zxfer_snapshot_discovery.sh`
- `test_zxfer_snapshot_reconcile.sh`
- `test_zxfer_property_reconcile.sh`
- `test_zxfer_replication.sh`
- `test_zxfer_send_receive.sh`

Some support modules are still covered inside adjacent suites. For example,
`src/zxfer_property_cache.sh` is exercised by
`test_zxfer_property_reconcile.sh` rather than a separate peer-named suite.
`src/zxfer_backup_metadata.sh` now has a dedicated peer suite in
`test_zxfer_backup_metadata.sh`, with a smaller number of cross-module backup
restore and remote-helper expectations still covered in the property and
remote-host suites.

The top-level launcher and `tests/test_helper.sh` both source
`src/zxfer_modules.sh`, so runtime module order is defined in one place rather
than being duplicated across test fixtures.

Focused tests that exercise `zxfer_init_globals()` should source through at
least the property-reconcile boundary, or anything later in
`src/zxfer_modules.sh`, because startup now resets property scratch state via
the property modules' public reset helpers rather than carrying a duplicated
copy of that reset inventory inside `zxfer_runtime.sh`.

The suites also use `tests/test_helper.sh` for the shared shunit2 scaffolding:
default no-op lifecycle hooks, temporary-directory setup helpers, and common
stdout/stderr/status capture wrappers for failure-path assertions. Keep new
suite-local helpers focused on domain-specific fixtures rather than re-creating
that generic test plumbing.

## Coverage

Generate shell coverage:

```sh
./tests/run_coverage.sh
```

The coverage runner prefers `kcov` when available and otherwise falls back to a
bash xtrace-based approximation.

That fallback now discounts shell syntax that bash xtrace cannot attribute to a
real command line, such as `case` labels, here-doc bodies/delimiters attached
to control-flow terminators, grouping delimiters, and multiline string
continuations.

The bash-xtrace path is also the enforcement path. It appends a `TOTAL` row to
`coverage/bash-xtrace/summary.tsv`, checks the current summary against the
committed minimums in `tests/coverage_policy.tsv`, rejects total or per-file
coverage regressions relative to
`tests/coverage_baseline/bash-xtrace/summary.tsv`, and writes a unified diff
against `tests/coverage_baseline/bash-xtrace/missing.txt` to
`coverage/bash-xtrace/missing.diff`.

Because bash xtrace coverage is an approximation and can vary slightly by shell
or platform, the no-regression comparison also allows a small committed
hit-count tolerance before it treats a lower percentage as a real regression.

Run the policy gate locally:

```sh
ZXFER_COVERAGE_MODE=bash-xtrace ./tests/run_coverage.sh
```

Bypass the gate when you intentionally need a fresh report before updating the
committed baseline:

```sh
ZXFER_COVERAGE_MODE=bash-xtrace ZXFER_COVERAGE_ENFORCE_POLICY=0 ./tests/run_coverage.sh
```

Locally, you can force the higher-fidelity path when `kcov` is installed:

```sh
ZXFER_COVERAGE_MODE=kcov ./tests/run_coverage.sh
```

## Integration Harness

Run the integration suite interactively:

```sh
./tests/run_integration_zxfer.sh
```

By default, the harness prompts before data-modifying wrapped external
commands. This is the safest mode when testing on a real workstation.

Run it unattended:

```sh
./tests/run_integration_zxfer.sh --yes
```

Keep running after failures:

```sh
./tests/run_integration_zxfer.sh --yes --keep-going
```

Skip one or more tests:

```sh
./tests/run_integration_zxfer.sh --yes --skip-test property_creation_with_zvol_test
```

Or:

```sh
ZXFER_SKIP_TESTS="property_creation_with_zvol_test property_override_and_ignore_test" \
./tests/run_integration_zxfer.sh --yes --keep-going
```

Useful environment variables:

- `ZXFER_BIN`
- `SPARSE_SIZE_MB`
- `TMPDIR`:
  must resolve to an absolute directory owned by root or the effective UID and
  must not be writable by other users unless the sticky bit is set, or zxfer
  will fall back to a validated default temp root, preferring memory-backed
  locations such as `/dev/shm` or `/run/shm` when available before falling
  back to the system temporary directory for scratch files, FIFOs, and caches
- `ZXFER_SKIP_TESTS`

## Safety Model

The integration harness is much safer than older versions:

- file-backed pools only
- sparse vdev files under the harness work tree
- marker-gated pool cleanup
- cleanup scoped to pools created by the current run

On macOS and Linux, the harness no longer hard-requires root, but it still
needs OpenZFS permissions that allow file-backed `zpool create` /
`zpool destroy`. On FreeBSD, root may still be required depending on module and
device setup.

But it is still not fully sandboxed. It performs real kernel ZFS operations and
real mounts on the host.

Recommended usage:

- local throwaway test host
- disposable VM
- dedicated CI runner

## GitHub Actions

The project currently ships four GitHub Actions workflows:

- `lint.yml`: `actionlint`, `checkbashisms`, ShellCheck, shfmt, and repository
  hygiene checks through the shared `tests/run_lint.sh` bootstrap with pinned
  tool versions and hashes
- `coverage.yml`: shell coverage with both the bash-xtrace fallback and a
  Docker-backed `kcov` pass, each uploaded as its own workflow artifact; the
  bash-xtrace lane is the coverage-policy gate and publishes the current
  `missing.txt` diff plus the policy report into the GitHub step summary
- `tests.yml`: shunit2 unit tests on Ubuntu and macOS, plus an Ubuntu
  portable-shell matrix for `dash`, `bash --posix`, `busybox ash`, and an
  initially non-blocking `posh` lane, plus dedicated FreeBSD and OmniOS
  VM-backed unit jobs
- `integration.yml`: integration tests on Ubuntu 24.04, FreeBSD, and OmniOS,
  with the Ubuntu, FreeBSD, and OmniOS lanes preserving the harness workdir on
  failure and uploading it as a workflow artifact

The integration job installs `zfsutils-linux`, loads the `zfs` module, and runs
the file-backed harness on `ubuntu-24.04` with `--yes --keep-going` so one
failure does not stop the rest of the integration pass. In CI it also sets
`ZXFER_PRESERVE_WORKDIR_ON_FAILURE=1`, so a failing run leaves its temporary
workdir under the job `TMPDIR` long enough for artifact upload.

The FreeBSD integration lane runs under `vmactions/freebsd-vm`, installs the
extra harness dependencies (`parallel` and `zstd`), attempts to load the
`zfs` module, and then executes the same file-backed harness with
`--yes --keep-going`. In CI it also sets
`ZXFER_PRESERVE_WORKDIR_ON_FAILURE=1` and points `TMPDIR` at a workspace-backed
directory so a failing run leaves the preserved workdir available for artifact
upload after the VM step returns.

Those integration lanes also run the harness's non-destructive fail-closed
security regressions. In addition to the existing shell-metacharacter path and
host-spec cases, the harness now feeds garbage wrapped host specs, remote
capability payloads with control-whitespace helper paths, and malformed remote
capability responses into the CLI startup path. Garbage wrapped host specs are
expected to abort before replication begins and before any injected marker
payload can be evaluated locally or through the mock SSH transport. Capability
payloads that contain malformed records or invalid helper paths are treated as
invalid handshakes: zxfer must not use or cache those helper paths, and it may
only continue by degrading safely to the direct remote `uname` / `command -v`
probe path. When that direct path is still valid, replication is expected to
complete successfully; when it is not, startup must fail closed. The harness
carries that fail-closed and fallback coverage across both the origin (`-O`)
and target (`-T`) remote startup paths so receive-side helper resolution is
exercised as well.

The backup-metadata integration cases are also current-format only. Positive
`-k` / `-e` restore scenarios first create the exact keyed metadata file
through live zxfer runs, then mutate that file for security or corruption
checks. Legacy mountpoint-local `.zxfer_backup_info.*` files are covered only
as fail-closed negative tests.

The OmniOS unit and integration lanes run under `vmactions/omnios-vm`, but they
do not use the exact same shell entry point. The integration harness still runs
under `/usr/xpg4/bin/sh` so the live-path illumos coverage matches the
project's supported POSIX shell expectations, while the shunit2 job wraps `bash
--posix` through `ZXFER_TEST_SHELL`. That distinction is intentional: OmniOS
`/usr/xpg4/bin/sh` follows ksh-style subshell function-binding semantics and
does not honor the helper overrides that the mock-heavy shunit2 suites use, so
the wrapper keeps the unit lane focused on zxfer behavior rather than shell-
specific test-stub dispatch.

The CI workflows use GitHub Actions concurrency cancellation keyed by workflow
name plus pushed ref, so stale branch runs are canceled when a new push
supersedes them.

The `kcov` job runs on `ubuntu-24.04` and uses the official `kcov/kcov` Docker
image pinned by digest instead of installing `kcov` from the runner package
manager. That keeps the higher-fidelity coverage lane available even though
current Ubuntu runner images do not consistently ship a native `kcov` package.
The bash-xtrace job is kept alongside it because the line-oriented
`summary.tsv`, `policy_failures.tsv`, and `missing.txt` diff outputs are stable
enough to enforce no-regression coverage policy in CI and on local developer
machines.

The macOS GitHub-hosted runner is currently used for `/bin/sh` and BSD-userland
unit coverage only. It is not a required hosted ZFS integration gate because
Darwin/OpenZFS property behavior remains less deterministic than
FreeBSD/Linux, as documented in [../KNOWN_ISSUES.md](../KNOWN_ISSUES.md).
The macOS shunit2 job intentionally does not install ZFS; it is meant to catch
shell and userland portability regressions in the mock-heavy unit suites.
The Ubuntu portable-shell matrix uses `ZXFER_TEST_SHELL` to rerun those same
shunit2 suites under alternate interpreters without changing the suite shebangs.
Hosted FreeBSD and OmniOS jobs now complement those Linux and macOS lanes, but
they do not replace local validation on the exact target OpenZFS and privilege
configuration used in production.
