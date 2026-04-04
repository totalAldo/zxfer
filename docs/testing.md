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

Run one suite:

```sh
./tests/run_shunit_tests.sh tests/test_zxfer_zfs_mode.sh
```

The test layout mirrors the source layout:

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

## Integration Harness

Run the integration suite interactively:

```sh
./tests/integration_zxfer.sh
```

By default, the harness prompts before data-modifying wrapped external
commands. This is the safest mode when testing on a real workstation.

Run it unattended:

```sh
./tests/integration_zxfer.sh --yes
```

Keep running after failures:

```sh
./tests/integration_zxfer.sh --yes --keep-going
```

Skip one or more tests:

```sh
./tests/integration_zxfer.sh --yes --skip-test property_creation_with_zvol_test
```

Or:

```sh
ZXFER_SKIP_TESTS="property_creation_with_zvol_test property_override_and_ignore_test" \
./tests/integration_zxfer.sh --yes --keep-going
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

The project currently ships three GitHub Actions workflows:

- `lint.yml`: ShellCheck, shfmt, and repository hygiene checks
- `tests.yml`: shunit2 unit tests
- `integration.yml`: Ubuntu integration tests with runtime ZFS setup

The integration job installs `zfsutils-linux`, loads the `zfs` module, and runs
the file-backed harness on `ubuntu-24.04` with `--yes --keep-going` so one
failure does not stop the rest of the integration pass.
