# TODO – Logical Bugs

- _None._


# TODO – Security Review

- **`eval` usage risks** – The codebase uses `eval` in several places (e.g., `src/zxfer_get_zfs_list.sh`, `src/zxfer_common.sh`) to execute commands. While variables are generally escaped, this pattern is inherently risky. Any failure in escaping logic could allow arbitrary code execution, especially when handling remote host arguments or dataset names with special characters.
- **Pipe error masking** – The script uses `#!/bin/sh` and does not appear to enable `set -o pipefail` (which is not portable to all `sh` implementations). This means that in pipelines like `cmd1 | cmd2`, if `cmd1` fails but `cmd2` succeeds, the pipeline's exit status is 0 (success). This could lead to silent failures, particularly in `src/zxfer_get_zfs_list.sh` where `zfs list` output is piped to `zstd` or `parallel`. If `zfs list` fails, the script might proceed with empty lists instead of aborting.

# TODO – Portability

- **Service Management (SMF) dependency** – The `-c` option (service control) relies on `svcadm` (`src/zxfer_zfs_mode.sh`), which is specific to Solaris, Illumos, and FreeBSD. This functionality will fail on Linux systems using `systemd` or `SysVinit`.
