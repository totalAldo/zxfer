# AGENTS

## Mission & Context
`zxfer` is a collection of POSIX shell scripts for high-reliability ZFS snapshot replication. The repository is optimized for FreeBSD/OpenZFS/Linux systems and manipulates real storage pools, so every change must preserve data integrity and remain transparent to administrators who depend on the tool in production.

## Environment & Constraints
- Primary shell is `/bin/sh`; assume POSIX features only. Bash-isms or GNU-only flags must be gated by compatibility checks.
- External tools (`zfs`, `zpool`, `ssh`, `gnu parallel`, `zstd`, `mktemp`, `comm`, `awk`) are assumed to exist but can vary by platform; guard optional dependencies with feature tests.
- Integration tests require root privileges, ZFS kernel modules, and enough disk space for sparse files. Note these prerequisites before requesting someone else to run them.
- Temporary files should reside in `/tmp/zxfer-*` (as done in current helpers) and be removed even on failure paths to prevent leaks on long-lived hosts.

## Priority Stack
1. **Safety** – never risk data loss or host instability.
2. **Security** – protect credentials, transports, and trust boundaries.
3. **Maintainability & Ease of Development** – keep the codebase simple to understand, test, and modify.
4. **Performance** – improve throughput only after the other priorities are satisfied and documented.

## Safety Expectations
- Treat every command as if it will run on a production host; avoid destructive ZFS operations unless explicitly scoped to throwaway sparse files created by the tests.
- Prefer reviewing helper functions in `src/zxfer_common.sh` and `src/zxfer_globals.sh` before re-implementing logic—many guardrails (argument validation, snapshot sanity checks, logging helpers) already exist.
- When a fix requires pool-level changes, rely on `tests/integration_zxfer.sh` which creates isolated temporary pools; never target live pools or datasets without the user’s confirmation.
- Fail closed: check command exit codes, propagate errors via `die()`/`log_err()` helpers, and default to aborting when state is uncertain.
- Document any behavior that could influence snapshot retention, deletion, or replication order so operators understand the blast radius.

## Security Expectations
- Keep `ssh` interactions hardened: respect control-socket reuse, avoid leaking private keys, and make sure any new options pass through quoting utilities already present in the scripts.
- Scrub external inputs (CLI flags, environment variables, remote dataset names). When expanding variables, prefer `${var:-}` patterns and guard against globbing or word-splitting.
- Avoid writing secrets to disk; if temporary files are required, lean on `mktemp` wrappers already provided and immediately `chmod 600` when storing sensitive data.
- Review calls to `sudo`/`ssh`/`zfs` for least privilege; never add new network endpoints or telemetry without an explicit design.

## Maintainability & Ease of Development
- Follow the modular layout under `src/`: functionality is grouped by concern (`*_zfs_send_receive.sh`, `*_inspect_delete_snap.sh`, etc.). Extend an existing module before creating a new file.
- Keep shell code POSIX-compliant; avoid Bash-isms because the scripts run with `/bin/sh` on BSD systems.
- Apply modern software-engineering practices: incremental commits, peer review mindset, automated lint/test runs, and clear commit messages that explain the "why" as well as the "what."
- Prefer small, testable functions with descriptive names and short pipelines; document any non-obvious `awk`, `comm`, or `gnu parallel` incantations with inline comments.
- Reduce cyclomatic complexity wherever possible by splitting large conditionals/loops into helpers and simplifying branching with early returns/guards.
- Run `shellcheck`/`shfmt` (when available) to keep style consistent, but do not require them for environments where they are missing—fall back to manual review.
- Update `README.md` and `CHANGELOG.txt` whenever behavior changes or new flags are introduced so downstream consumers can track compatibility.
- Ensure unit helpers stay covered by `./tests/test_zxfer_common.sh`. Add or update shunit2 cases when modifying public functions.

## Performance (last, but deliberate)
- Only pursue concurrency tweaks (e.g., adjusting `-j`, gnu parallel usage, compression flags) after validating safety/security and explaining trade-offs.
- Measure before optimizing. Capture representative timings during integration tests and summarize them in the PR or commit message.
- Keep resource usage configurable (env vars or flags) instead of hard-coding aggressive defaults.

## Patterns & Best Practices for Codex
- **Collect context first:** read `README.md`, `CHANGELOG.txt`, and any scripts relevant to the task before editing. Use `rg` for targeted searches and avoid assumptions about legacy behavior.
- **Plan before executing:** outline the approach (especially for anything touching ZFS send/receive) and share it with the user when changes affect replication semantics or dataset deletion.
- **Edit safely:** prefer `apply_patch` for small changes, keep modifications minimal, and never revert user edits unless asked. When updating shell code, mirror the project’s indentation (tabs) and quoting style.
- **Validate continuously:** run `./tests/test_zxfer_common.sh` after editing helpers; reserve `sudo ./tests/integration_zxfer.sh` for when ZFS modules and root access are available. Mention when tests were skipped and why.
- **Explain trade-offs:** whenever a change impacts the priority stack, call out how safety/security were preserved, how maintainability was affected, and why performance adjustments are justified.
- **Document artifacts:** update comments and docs alongside code so future contributors can follow the rationale without reverse-engineering the change.

### Information Codex Needs Up Front
- Target platform (FreeBSD vs. Linux) and whether root/sudo is available for running integration tests or issuing `zfs` commands.
- Whether tests can touch real pools or must be confined to sparse-file simulations; include dataset names and any redacted hostnames to avoid accidents.
- Expected user-facing behavior (flags, error messages, compatibility requirements) so documentation and changelog entries stay accurate.
- Any performance expectations (max concurrency, bandwidth caps, compression defaults) to evaluate trade-offs against the priority stack.
- Confirmation that optional tooling (`gnu parallel`, `zstd`, `shellcheck`) is installed when a proposed change depends on it; otherwise plan for fallbacks.
