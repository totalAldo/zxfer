# AGENTS

## Mission & Context
`zxfer` is a collection of POSIX shell scripts for high-reliability ZFS snapshot replication. The repository targets FreeBSD, Linux/OpenZFS, Solaris/illumos, and current OpenZFS-on-macOS workflows. It manipulates real storage pools and remote hosts, so every change must preserve data integrity and remain transparent to administrators who depend on the tool in production. Its CLI behavior, operator-visible output, and replication semantics are public interfaces; compatibility changes must be intentional, documented, and tested.

## Environment & Constraints
- Primary shell is `/bin/sh`; assume POSIX features only. Bash-isms or GNU-only flags must be gated by compatibility checks.
- Treat [`docs/coding-style.md`](docs/coding-style.md) as the repository style authority for shell naming, formatting, quoting, module layout, and validation expectations.
- Respect `.editorconfig` for cross-repo formatting: shell sources use tabs, while docs and workflow-style files use their existing space-indented layout.
- Keep `src/` flat and preserve [`src/zxfer_modules.sh`](src/zxfer_modules.sh) as the single source-order authority for the launcher and direct-sourcing tests.
- External tools (`zfs`, `zpool`, `ssh`, `gnu parallel`, `zstd`, `mktemp`, `comm`, `awk`) are assumed to exist but can vary by platform; guard optional dependencies with feature tests.
- zxfer now rebuilds `PATH` from a trusted allowlist and resolves required helpers to absolute paths. If a change touches dependency lookup or remote helper execution, preserve `ZXFER_SECURE_PATH` / `ZXFER_SECURE_PATH_APPEND` behavior locally and over `-O/-T`.
- Non-zero exits now emit a structured stderr failure report and may optionally mirror it to `ZXFER_ERROR_LOG`. Keep new error paths compatible with that centralized reporting flow.
- Integration tests use file-backed sparse pools rooted under a temporary `WORKDIR`. On macOS and Linux they no longer hard-require root, but they still require OpenZFS permissions that allow `zpool create` / `zpool destroy`; on FreeBSD root may still be required depending on device/module setup. Keep the harness current when behavior changes. An automated agent may run the VM-backed integration entrypoint `tests/run_vm_matrix.sh` automatically only with host-friendly local profiles such as `--profile smoke` or `--profile local`, and must never invoke `tests/run_integration_zxfer.sh` directly on the host. Broader VM-matrix runs such as `--profile full`, `--profile ci`, or slow emulated guests remain manual-only.
- `tests/run_integration_zxfer.sh` prompts for approval before data-modifying wrapped external commands by default. Update the script and its expectations when behavior changes. Direct host execution of `tests/run_integration_zxfer.sh`, including any `--yes` usage, remains human-only; when an automated agent runs integration automatically it must do so through `tests/run_vm_matrix.sh`, which runs the harness inside the guest.
- Temporary files, FIFOs, queues, and cache directories should use the runtime helpers in `src/zxfer_runtime.sh` and be removed on both success and failure paths unless they are intentionally preserved for debugging.

## Priority Stack
1. **Safety** – never risk data loss or host instability.
2. **Security** – protect credentials, transports, and trust boundaries.
3. **Maintainability & Ease of Development** – keep the codebase simple to understand, test, and modify.
4. **Performance** – improve throughput only after the other priorities are satisfied and documented.

## Safety Expectations
- Treat every command as if it will run on a production host; avoid destructive ZFS operations unless explicitly scoped to throwaway sparse files created by the tests.
- Prefer reviewing the focused helpers in `src/zxfer_reporting.sh`, `src/zxfer_exec.sh`, `src/zxfer_runtime.sh`, `src/zxfer_cli.sh`, `src/zxfer_dependencies.sh`, `src/zxfer_remote_hosts.sh`, `src/zxfer_path_security.sh`, and `src/zxfer_snapshot_state.sh` before re-implementing logic—many guardrails (argument validation, quoting, dependency lookup, snapshot sanity checks, logging helpers) already exist.
- When a fix requires pool-level changes, use `tests/run_integration_zxfer.sh` as the integration harness to review and update. An automated agent may execute that harness only through `tests/run_vm_matrix.sh`, only when the pool activity stays inside a disposable VM guest, and only with host-friendly local profiles unless the user explicitly asks for a broader manual run. Never run `tests/run_integration_zxfer.sh` directly on the host, and never target live pools or datasets without the user’s confirmation.
- Keep the integration harness file-backed only. Do not add raw-device, loopback-device, or host-import/export paths without a very strong reason and explicit documentation.
- The integration harness is safer than before, but it still performs real kernel ZFS operations. For “zero host risk” requests, recommend a disposable VM rather than claiming the harness is fully sandboxed.
- Fail closed: check command exit codes, route operator-facing failures through the reporting helpers such as `zxfer_throw_error*`, preserve structured failure metadata, and default to aborting when state is uncertain.
- Document any behavior that could influence snapshot retention, deletion, or replication order so operators understand the blast radius.

## Security Expectations
- Keep `ssh` interactions hardened: respect control-socket reuse, avoid leaking private keys, and make sure any new options pass through quoting utilities already present in the scripts.
- Preserve support for wrapper-style host specs such as `user@host pfexec` or `user@host doas`; do not collapse them into a single hostname or reintroduce shell-injection surfaces in remote command construction.
- Scrub external inputs (CLI flags, environment variables, remote dataset names). When expanding variables, prefer `${var:-}` patterns and guard against globbing or word-splitting.
- Avoid writing secrets to disk; if temporary files are required, lean on `mktemp` wrappers already provided and immediately `chmod 600` when storing sensitive data.
- Review calls to `sudo`/`ssh`/`zfs` for least privilege; never add new network endpoints or telemetry without an explicit design.

## Maintainability & Ease of Development
- Follow the flat modular layout under `src/`: functionality is grouped by stable concern (`zxfer_send_receive.sh`, `zxfer_snapshot_reconcile.sh`, `zxfer_property_reconcile.sh`, etc.). Extend an existing module before creating a new file, avoid generic filenames such as `common`, `globals`, `utils`, or `lib`, and keep `src/zxfer_modules.sh` as the only source-order authority.
- Major `src/` modules should start with a short `Module contract` comment block that summarizes `owns globals`, `reads globals`, `mutates caches`, and `returns via stdout`. Keep those headers short and focused on ownership boundaries and data flow.
- Keep shell code POSIX-compliant; avoid Bash-isms because the scripts run with `/bin/sh` on BSD systems.
- Use the project naming conventions consistently: shared helpers use `zxfer_`, global state uses `g_`, parsed option state uses `g_option_*`, function-scoped temporaries use `l_`, and operator-facing environment variables remain `ZXFER_*`.
- Top-level functions in `src/` modules should have the current short structured comment form with `Purpose:` and `Usage:`, plus `Returns:` or `Side effects:` when the contract is not obvious. When you change a function's contract, update that comment block and any still-relevant nearby rationale comments in the same change.
- Apply modern software-engineering practices: incremental commits, peer review mindset, automated lint/test runs, and clear commit messages that explain the "why" as well as the "what."
- Treat flags, positional arguments, environment variables, help text, exit codes, stdout/stderr formats, structured error reports, and replication/retention behavior as public interfaces. Do not change them silently; document compatibility impact and add or adjust regression coverage when they move.
- Prefer small, testable functions with descriptive names and short pipelines; prefer early returns over deep nesting; and document any non-obvious `awk`, `sed`, `comm`, `gnu parallel`, ssh, or quoting logic with short comments that explain why the block exists.
- Reduce cyclomatic complexity wherever possible by splitting large conditionals/loops into helpers and simplifying branching with early returns/guards.
- Preserve argument boundaries and quoting discipline. Reuse the centralized helpers in `src/zxfer_exec.sh`, `src/zxfer_dependencies.sh`, and `src/zxfer_remote_hosts.sh` instead of adding ad hoc `eval`, helper lookup, or remote-command construction paths.
- Keep source-time side effects minimal. Runtime setup should happen in explicit init flows, not merely because a module was sourced.
- Use `./tests/run_lint.sh` as the authoritative pinned lint entrypoint because it mirrors CI's toolchain and checks. Ad hoc `shellcheck` or `shfmt` runs are useful for iteration but do not replace the repository lint runner.
- Whenever a feature is added or existing behavior, flags, defaults, workflows, error messages, or contributor/security expectations change, review all user-facing docs for drift and update them in the same change. At minimum this includes `README.md`, `CHANGELOG.txt`, `CONTRIBUTING.md`, `SECURITY.md`, `KNOWN_ISSUES.md` when applicable, the relevant docs under `docs/`, `examples/README.md`, `packaging/README.txt`, the man pages under `man/`, and any examples or inline help that describe the affected functionality.
- When modifying replication logic, state initialization, or adding new features, ensure the corresponding Mermaid diagrams in `architecture.md` and `README.md` are updated to reflect the new control flow.
- When behavior differs by platform or OpenZFS variant, document the affected platforms explicitly and keep tests gated or annotated so the expected differences are visible rather than implied.
- When commands, dependencies, installed paths, test entry points, or release expectations change, review related packaging and automation artifacts for drift as part of the same change. This includes files under `packaging/`, `.github/workflows/`, and `.github/PULL_REQUEST_TEMPLATE.md` when they describe or enforce the affected behavior.
- Keep tests aligned with shipped behavior. The main shunit2 suites live under `tests/test_*.sh`; add or update focused coverage when modifying public helpers or replication control flow, use `tests/test_helper.sh` before adding new suite-local scaffolding, and adjust integration or regression coverage when behavior changes so stale expectations do not linger, even when the integration harness will be run manually later.
- `tests/run_coverage.sh` is available and should be kept working; it prefers `kcov` and falls back to a bash xtrace approximation, and the bash-xtrace lane is the current enforcement path. When coverage behavior or expectations change, review `tests/coverage_policy.tsv` and `tests/coverage_baseline/bash-xtrace/` in the same change instead of treating generated coverage output as incidental.

## Performance (last, but deliberate)
- Only pursue concurrency tweaks (e.g., adjusting `-j`, gnu parallel usage, compression flags) after validating safety/security and explaining trade-offs.
- Measure before optimizing. Capture representative timings during manual integration tests when they are performed and summarize them in the PR or commit message.
- Keep resource usage configurable (env vars or flags) instead of hard-coding aggressive defaults.

## Patterns & Best Practices for Agents
- **Collect context first:** read `README.md`, `CHANGELOG.txt`, the relevant man page/example, the scripts you plan to touch, and the matching `tests/test_*.sh` coverage before editing. Also read `docs/testing.md`, `docs/platforms.md`, `docs/architecture.md`, `KNOWN_ISSUES.md`, and `SECURITY.md` when the change touches validation flow, platform behavior, architecture/state ownership, known open risks, or trust boundaries. Use `rg` for targeted searches and avoid assumptions about legacy behavior.
- **Plan before executing:** outline the approach (especially for anything touching ZFS send/receive) and share it with the user when changes affect replication semantics or dataset deletion.
- **Edit safely:** prefer `apply_patch` for small changes, keep modifications minimal, and never revert user edits unless asked. When updating shell code, mirror the project’s indentation (tabs), naming (`zxfer_`, `g_`, `g_option_*`, `l_`), and quoting style, and prefer the shared execution, dependency, and reporting helpers over new ad hoc plumbing.
- **Validate continuously:** when changing shell logic, run `./tests/run_shunit_tests.sh`, `./tests/run_lint.sh`, and `ZXFER_COVERAGE_MODE=bash-xtrace ./tests/run_coverage.sh`. Use targeted suites for faster iteration before the full pass. When the work changes coverage behavior or the expected covered surface, review `tests/coverage_policy.tsv` and `tests/coverage_baseline/bash-xtrace/` deliberately rather than only looking at generated reports. An automated agent may additionally run `tests/run_vm_matrix.sh` when a disposable guest boundary is available and the work benefits from automatic integration coverage, but must not run `tests/run_integration_zxfer.sh` directly on the host. Automatic VM-backed validation should stay on `--profile smoke` or `--profile local`; do not have the agent auto-run `--profile full`, `--profile ci`, or slow emulated guests such as OmniOS on macOS/arm64 hosts. When integration coverage is needed during iteration, tighten the loop first with `tests/run_vm_matrix.sh --profile local --guest ... --only-test ...` so the agent reruns only the affected in-guest cases before widening back out manually when a human chooses to do so.
- **Explain trade-offs:** whenever a change impacts the priority stack, call out how safety/security were preserved, how maintainability was affected, and why performance adjustments are justified.
- **Document artifacts:** treat documentation, packaging metadata, CI metadata, top-level contributor/security docs, and test review as part of implementation. When behavior changes, verify the relevant docs, man pages, examples, `CONTRIBUTING.md`, `SECURITY.md`, `KNOWN_ISSUES.md` when applicable, packaging files, workflow files, comments, and tests still match the code, update them together, and if no doc or test update is needed, say why explicitly.

### Information Agents Need Up Front
- Target platform (FreeBSD, Linux, illumos/Solaris, or OpenZFS-on-macOS) and whether the current user can create/destroy file-backed zpools without `sudo`.
- Whether tests can touch real pools or must be confined to the integration harness’s sparse-file pools; include dataset names and any redacted hostnames to avoid accidents.
- Expected user-facing behavior (flags, error messages, compatibility requirements) so documentation and changelog entries stay accurate.
- Whether the change affects packaging, CI, installation paths, or release artifacts so related files under `packaging/` and `.github/` can be reviewed and updated deliberately.
- Any performance expectations (max concurrency, bandwidth caps, compression defaults) to evaluate trade-offs against the priority stack.
- Confirmation that optional tooling (`gnu parallel`, `zstd`, `shellcheck`, `shfmt`, `kcov`) is installed when a proposed change depends on it; otherwise plan for fallbacks.
