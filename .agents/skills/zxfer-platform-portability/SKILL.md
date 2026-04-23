---
name: zxfer-platform-portability
description: Review zxfer changes for POSIX /bin/sh portability and ZFS/OpenZFS platform compatibility across FreeBSD, Linux/OpenZFS, illumos/Solaris, and OpenZFS-on-macOS. Use when code touches shell syntax, external tool flags, zfs/zpool behavior, ssh or remote wrappers, secure PATH, temp/runtime helpers, or optional dependencies.
---

# zxfer Platform Portability

## Context to Load

- Read `AGENTS.md`, `docs/coding-style.md`, `docs/platforms.md`, and `docs/external-tools.md`.
- Inspect the relevant source module and peer tests before proposing a portability fix.
- For runtime artifacts, also inspect `src/zxfer_runtime.sh`.
- For remote execution, also inspect `src/zxfer_exec.sh`, `src/zxfer_dependencies.sh`, and `src/zxfer_remote_hosts.sh`.

## Checks

- Keep shell code POSIX `/bin/sh`; reject Bash-only constructs such as arrays, `[[ ]]`, `local`, process substitution, here-strings, and `$'...'`.
- Gate GNU-only or platform-specific command flags behind feature checks.
- Preserve wrapper-style remote host specs and argument boundaries.
- Preserve secure helper lookup through `ZXFER_SECURE_PATH` and `ZXFER_SECURE_PATH_APPEND`.
- Use runtime helpers for temporary files, FIFOs, queues, cache files, traps, and cleanup.
- Keep structured failure reporting and `ZXFER_ERROR_LOG` behavior compatible.
- Document FreeBSD, Linux/OpenZFS, illumos/Solaris, and macOS differences when behavior diverges.

## Output

- Identify the affected platform or shell family for each portability concern.
- Prefer small helpers and early returns over broad rewrites.
- Pair portability fixes with targeted tests and docs updates when behavior or expectations change.
