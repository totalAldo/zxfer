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

Run one suite:

```sh
./tests/run_shunit_tests.sh tests/test_zxfer_zfs_mode.sh
```

Run the suites under a specific alternate shell:

```sh
ZXFER_TEST_SHELL=/bin/dash ./tests/run_shunit_tests.sh
```

For multi-word shell modes such as `bash --posix`, point `ZXFER_TEST_SHELL` at
an executable wrapper script that `exec`s the desired command.

The test layout mirrors the source layout:

- `test_run_shunit_tests.sh`
- `test_zxfer_common.sh`
- `test_zxfer_globals.sh`
- `test_zxfer_get_zfs_list.sh`
- `test_zxfer_inspect_delete_snap.sh`
- `test_zxfer_transfer_properties.sh`
- `test_zxfer_zfs_mode.sh`
- `test_zxfer_zfs_send_receive.sh`

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
- `TMPDIR`
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
- `tests.yml`: shunit2 unit tests on `ubuntu-latest` and `macos-latest`, plus
  an Ubuntu portable-shell matrix for `dash`, `bash --posix`, `busybox ash`,
  and an initially non-blocking `posh` lane
- `integration.yml`: Ubuntu integration tests with runtime ZFS setup, preserving
  the harness workdir on failure and uploading it as a workflow artifact

The integration job installs `zfsutils-linux`, loads the `zfs` module, and runs
the file-backed harness on `ubuntu-24.04` with `--yes --keep-going` so one
failure does not stop the rest of the integration pass. In CI it also sets
`ZXFER_PRESERVE_WORKDIR_ON_FAILURE=1`, so a failing run leaves its temporary
workdir under the job `TMPDIR` long enough for artifact upload.

The CI workflows use GitHub Actions concurrency cancellation keyed by workflow
name plus pull request number or ref, so stale branch runs are canceled when a
new push supersedes them.

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
