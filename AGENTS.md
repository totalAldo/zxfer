# AGENTS

## Mission & Context
`zxfer` is a collection of POSIX shell scripts for high-reliability ZFS snapshot replication. The repository targets FreeBSD, Linux/OpenZFS, Solaris/illumos, and current OpenZFS-on-macOS workflows. It manipulates real storage pools and remote hosts, so every change must preserve data integrity and remain transparent to administrators who depend on the tool in production.

## Environment & Constraints
- Primary shell is `/bin/sh`; assume POSIX features only. Bash-isms or GNU-only flags must be gated by compatibility checks.
- External tools (`zfs`, `zpool`, `ssh`, `gnu parallel`, `zstd`, `mktemp`, `comm`, `awk`) are assumed to exist but can vary by platform; guard optional dependencies with feature tests.
- zxfer now rebuilds `PATH` from a trusted allowlist and resolves required helpers to absolute paths. If a change touches dependency lookup or remote helper execution, preserve `ZXFER_SECURE_PATH` / `ZXFER_SECURE_PATH_APPEND` behavior locally and over `-O/-T`.
- Non-zero exits now emit a structured stderr failure report and may optionally mirror it to `ZXFER_ERROR_LOG`. Keep new error paths compatible with that centralized reporting flow.
- Integration tests use file-backed sparse pools rooted under a temporary `WORKDIR`. On macOS and Linux they no longer hard-require root, but they still require OpenZFS permissions that allow `zpool create` / `zpool destroy`; on FreeBSD root may still be required depending on device/module setup.
- `tests/run_integration_zxfer.sh` prompts for approval before data-modifying wrapped external commands by default. Use `--yes` only when the user explicitly wants unattended execution.
- Temporary files should use `${TMPDIR:-/tmp}` via the existing helpers and be removed even on failure paths to prevent leaks on long-lived hosts.

## Priority Stack
1. **Safety** – never risk data loss or host instability.
2. **Security** – protect credentials, transports, and trust boundaries.
3. **Maintainability & Ease of Development** – keep the codebase simple to understand, test, and modify.
4. **Performance** – improve throughput only after the other priorities are satisfied and documented.

## Safety Expectations
- Treat every command as if it will run on a production host; avoid destructive ZFS operations unless explicitly scoped to throwaway sparse files created by the tests.
- Prefer reviewing helper functions in `src/zxfer_common.sh` and `src/zxfer_globals.sh` before re-implementing logic—many guardrails (argument validation, snapshot sanity checks, logging helpers) already exist.
- When a fix requires pool-level changes, rely on `tests/run_integration_zxfer.sh`, which creates isolated file-backed temporary pools under `WORKDIR`; never target live pools or datasets without the user’s confirmation.
- Keep the integration harness file-backed only. Do not add raw-device, loopback-device, or host-import/export paths without a very strong reason and explicit documentation.
- The integration harness is safer than before, but it still performs real kernel ZFS operations. For “zero host risk” requests, recommend a disposable VM rather than claiming the harness is fully sandboxed.
- Fail closed: check command exit codes, propagate errors via `die()`/`log_err()` helpers, and default to aborting when state is uncertain.
- Document any behavior that could influence snapshot retention, deletion, or replication order so operators understand the blast radius.

## Security Expectations
- Keep `ssh` interactions hardened: respect control-socket reuse, avoid leaking private keys, and make sure any new options pass through quoting utilities already present in the scripts.
- Preserve support for wrapper-style host specs such as `user@host pfexec` or `user@host doas`; do not collapse them into a single hostname or reintroduce shell-injection surfaces in remote command construction.
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
- Update `README.md`, `CHANGELOG.txt`, and the man pages when behavior changes or new flags are introduced so downstream consumers can track compatibility.
- Keep shunit2 coverage current. The main suites live under `tests/test_*.sh`; add or update focused coverage when modifying public helpers or replication control flow.
- `tests/run_coverage.sh` is available and should be kept working; it prefers `kcov` and falls back to a bash xtrace approximation.

## Performance (last, but deliberate)
- Only pursue concurrency tweaks (e.g., adjusting `-j`, gnu parallel usage, compression flags) after validating safety/security and explaining trade-offs.
- Measure before optimizing. Capture representative timings during integration tests and summarize them in the PR or commit message.
- Keep resource usage configurable (env vars or flags) instead of hard-coding aggressive defaults.

## Patterns & Best Practices for Codex
- **Collect context first:** read `README.md`, `CHANGELOG.txt`, and any scripts relevant to the task before editing. Use `rg` for targeted searches and avoid assumptions about legacy behavior.
- **Plan before executing:** outline the approach (especially for anything touching ZFS send/receive) and share it with the user when changes affect replication semantics or dataset deletion.
- **Edit safely:** prefer `apply_patch` for small changes, keep modifications minimal, and never revert user edits unless asked. When updating shell code, mirror the project’s indentation (tabs) and quoting style.
- **Validate continuously:** run `./tests/run_shunit_tests.sh` after meaningful shell changes. Use targeted suites for faster iteration, run `tests/run_coverage.sh` when expanding tests, and only use `./tests/run_integration_zxfer.sh --yes` when the environment truly has safe file-backed ZFS test permissions. Mention when integration tests were skipped and why.
- **Explain trade-offs:** whenever a change impacts the priority stack, call out how safety/security were preserved, how maintainability was affected, and why performance adjustments are justified.
- **Document artifacts:** update comments and docs alongside code so future contributors can follow the rationale without reverse-engineering the change.

### Information Codex Needs Up Front
- Target platform (FreeBSD, Linux, illumos/Solaris, or OpenZFS-on-macOS) and whether the current user can create/destroy file-backed zpools without `sudo`.
- Whether tests can touch real pools or must be confined to the integration harness’s sparse-file pools; include dataset names and any redacted hostnames to avoid accidents.
- Expected user-facing behavior (flags, error messages, compatibility requirements) so documentation and changelog entries stay accurate.
- Any performance expectations (max concurrency, bandwidth caps, compression defaults) to evaluate trade-offs against the priority stack.
- Confirmation that optional tooling (`gnu parallel`, `zstd`, `shellcheck`, `shfmt`, `kcov`) is installed when a proposed change depends on it; otherwise plan for fallbacks.
