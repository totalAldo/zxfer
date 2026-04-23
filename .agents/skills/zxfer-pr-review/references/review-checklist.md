# zxfer Review Checklist

Use this checklist as a focused review aid. Do not force every item into the final response; report only actionable findings.

## Safety and Data Integrity

- Check snapshot selection, ordering, rollback, deletion, and retention behavior for unintended data loss.
- Confirm uncertain ZFS state fails closed and routes operator-facing errors through reporting helpers.
- Verify integration harness changes stay file-backed and do not introduce host device, raw disk, or host import/export paths.

## Security and Trust Boundaries

- Check remote command construction for preserved argument boundaries and wrapper host specs such as `user@host pfexec`.
- Verify helper lookup preserves `ZXFER_SECURE_PATH` and `ZXFER_SECURE_PATH_APPEND` locally and over `-O` / `-T`.
- Look for new `eval`, unquoted expansions, secret persistence, unsafe temp files, or telemetry/network endpoints.

## Portability and Public Interfaces

- Check for POSIX `/bin/sh` compliance and gated platform-specific flags or output parsing.
- Treat flags, env vars, exit codes, stdout/stderr formats, structured failure reports, docs, examples, and man pages as public interfaces.
- Confirm platform differences are documented explicitly when FreeBSD, Linux/OpenZFS, illumos/Solaris, or macOS behavior diverges.

## Tests and Documentation

- Require focused shunit2 coverage for changed helpers or public behavior.
- Require lint and coverage policy consideration when shell logic or coverage tooling changes.
- Require docs, examples, man pages, packaging, workflow, or diagram updates when behavior, commands, defaults, or release expectations move.
