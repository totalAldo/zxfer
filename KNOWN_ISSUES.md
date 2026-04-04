# KNOWN ISSUES

This file tracks open issues that still matter for current releases. Resolved
historical bugs should be removed instead of accumulated here.

## Open Functional Issues

- Remote OS detection still goes through `eval` and the remote login shell's default `PATH`.
  Files: `src/zxfer_common.sh` (`get_os()`), `src/zxfer_globals.sh` (`init_variables()`).
  Impact: remote platform detection can execute the remote account's `uname` instead of a securely resolved binary, and shell behavior differences remain in play earlier than the hardened remote `zfs`/`cat` lookup paths. This mostly affects feature gating and platform-specific property handling, not the main replication stream.
  Recommended fix: replace `get_os()` with the same argv-based ssh helper path used by `run_source_zfs_cmd()` / `run_destination_zfs_cmd()`, or resolve `uname` remotely through the secure PATH before use.

- Remote source snapshot listing with `-O ... -j ... -z` still invokes remote `zstd` by bare name.
  File: `src/zxfer_get_zfs_list.sh` (`build_source_snapshot_list_cmd()`).
  Impact: mixed-platform hosts can still fail source snapshot discovery if `zstd` is not present in the remote login shell `PATH`, or can execute an unintended remote binary if that `PATH` is compromised. Remote `zfs` and GNU `parallel` are now securely resolved, but remote compression is not.
  Recommended fix: resolve `zstd` remotely through the same secure-PATH mechanism used for `zfs`, `cat`, and GNU `parallel`, then build the remote listing pipeline with that absolute path.

- OpenZFS-on-macOS property integration remains less deterministic than FreeBSD/Linux for some inherited child-dataset properties.
  File: `tests/run_integration_zxfer.sh` (`property_creation_with_zvol_test()`, `property_override_and_ignore_test()`).
  Impact: Darwin integration tests currently skip the strict child `atime=off` assertions because that behavior has not been made stable enough to use as a portable end-to-end gate. This is currently treated as a platform-specific certification gap rather than a proven production data-loss bug.
  Recommended fix: investigate the exact property-source/value differences on Darwin after receive and property reconciliation, then either normalize zxfer's post-receive behavior or narrow the documented expectations for that platform.

- FreeBSD still reports some created or updated dataset properties with unexpected source classifications (`default` versus `local`).
  Files: `src/zxfer_globals.sh` (`g_fbsd_readonly_properties`), `src/zxfer_transfer_properties.sh` (`transfer_properties()`).
  Impact: properties such as `quota`, `reservation`, `canmount`, `refquota`, and `refreservation` can land with a different property source than operators might expect after `zfs create` / `zfs set`. This does not currently affect inheritable-property reconciliation, but it remains a platform-specific behavior difference worth documenting.
  Recommended fix: keep FreeBSD-specific assertions narrow in tests and investigate whether post-create reconciliation can normalize the reported property source without introducing cross-platform regressions.

## Security Review

- The codebase still relies on `eval` for several command-construction paths.
  Files: `src/zxfer_common.sh` (`get_os()`, `execute_command()`, `execute_background_cmd()`, `exists_destination()`), `src/zxfer_get_zfs_list.sh` (source/destination list execution).
  Risk: current quoting helpers and tokenization rules significantly reduce direct injection risk, but this remains an architecture-sensitive area. A future change that passes partially quoted or newly user-controlled text into these helpers could reopen command-injection bugs.
  Recommended direction: prefer argv-based execution helpers wherever practical, and treat any new `eval` call or new `l_cmd="..."` string as a security-sensitive review point.

- Remote trust-boundary hardening is incomplete outside the main `zfs`/`cat`/`parallel` paths.
  Files: `src/zxfer_common.sh` (`get_os()`), `src/zxfer_get_zfs_list.sh` (remote `zstd` pipeline).
  Risk: zxfer's secure PATH model is strong for the core replication helpers, but a compromised remote login shell can still influence `uname` and `zstd` resolution in these remaining paths.
  Recommended direction: eliminate default-shell PATH dependence for remote helper execution, or document the residual trust assumptions more explicitly in the user docs.

## Testing Limitations

- `tests/run_integration_zxfer.sh` is file-backed and considerably safer than earlier versions, but it is not fully sandboxed.
  File: `tests/run_integration_zxfer.sh`.
  Impact: the harness avoids raw devices and should only create/destroy its own file-backed pools, but it still performs real kernel ZFS operations, mount activity, and dataset changes on the host.
  Current state: this is partially mitigated by the dedicated GitHub Actions integration workflow, which now runs the file-backed harness on `ubuntu-24.04` with ZFS installed at job runtime.
  Operational guidance: use a disposable VM, throwaway host, or CI runner for zero-risk validation; do not describe the harness as fully sandboxed.
