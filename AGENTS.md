# AGENTS

## Mission and Completion Bar

`zxfer` is a collection of POSIX shell scripts for high-reliability ZFS
snapshot replication across FreeBSD, Linux/OpenZFS, Solaris/illumos, and
current OpenZFS-on-macOS workflows. It manipulates real pools, datasets, and
remote hosts. Treat data integrity, operator trust, and compatibility as
release requirements.

A change is complete only when it:

- satisfies the requested behavior without unrelated scope expansion;
- preserves safety, security, POSIX portability, and public interfaces unless
  an intentional compatibility change is documented;
- includes focused regression coverage and operator-facing documentation when
  behavior changes;
- passes the relevant host-safe validation, or clearly records what was not
  run and why; and
- has been self-reviewed for data-loss risk, command-injection risk, failure
  propagation, cleanup, cross-platform behavior, and documentation drift.

## Priority Order

1. **Safety** — never risk data loss or host instability.
2. **Security** — protect credentials, transports, paths, and trust boundaries.
3. **Maintainability** — keep behavior understandable, testable, and explicit.
4. **Performance** — optimize only after the first three are preserved and the
   trade-off is measured.

## Scope, Authority, and Communication

- For requests to answer, explain, review, diagnose, or plan, inspect the
  relevant repository state and report the result. Do not edit unless the
  request also asks for a change.
- For requests to change, build, fix, or update, make the smallest coherent
  in-scope edits and run relevant non-destructive validation without asking
  first.
- Safe local reads, in-scope edits, and host-safe unit/lint/coverage commands
  are authorized by an implementation request. Require confirmation before
  touching live pools or datasets, writing to an external service, adding a
  production dependency, or materially expanding the requested scope. The
  stricter integration-test prohibition below still applies.
- Inspect `git status` and the focused diff before editing. Preserve user-owned
  changes, including overlapping work, and never discard or rewrite them merely
  to simplify the task.
- Gather targeted repository context before asking questions. Infer from the
  task, code, tests, and docs when safe; ask only when a missing choice would
  materially change behavior, compatibility, or risk.
- For multi-step work, keep a short visible checklist. Send concise updates at
  phase changes or when evidence changes the plan; do not narrate routine tool
  calls.
- Lead the final handoff with the outcome, then validation performed, checks not
  run, compatibility impact, and residual risk.

## Codex and GPT-5.6 Guidance

- Keep this file focused on durable repository facts. Put richer repeatable
  workflows in `.agents/skills/`, temporary task constraints in the prompt,
  and intentional project-wide Codex settings in `.codex/config.toml`.
- Write instructions for GPT-5.6-class agents in terms of the outcome, relevant
  context, hard constraints, success criteria, and stop conditions. State each
  invariant once, avoid contradictory or ceremonial process rules, and leave
  implementation-path choices to the agent when safety does not prescribe one.
- Do not pin a model or reasoning effort in `AGENTS.md`. If the project adopts a
  shared model or effort default, configure it in Codex configuration and
  compare it on representative zxfer tasks. Higher reasoning effort is not a
  substitute for precise constraints, source evidence, or validation; reserve
  maximum effort for unusually difficult quality-first work where it shows a
  measured benefit.
- When Codex behavior, OpenAI model behavior, or configuration semantics matter,
  consult current official OpenAI/Codex documentation instead of relying on
  remembered release details.

## Context and Tool Routing

- Start with `rg`, the focused diff, and the smallest set of relevant source,
  test, and documentation files. Read `README.md`, `CHANGELOG.txt`, the relevant
  man page or example, the changed modules, and peer `tests/test_*.sh` coverage
  before changing shipped behavior.
- Read `docs/testing.md` for validation changes, `docs/platforms.md` for
  compatibility work, `docs/architecture.md` for module/state ownership,
  `SECURITY.md` for trust-boundary changes, and `KNOWN_ISSUES.md` when resolving
  or discovering an open risk.
- Use the matching repository skill when applicable:
  - `zxfer-pr-review` for branch, PR, commit, or working-tree reviews;
  - `zxfer-platform-portability` for shell, command, ZFS, or platform-sensitive
    changes;
  - `zxfer-validation-plan` to select safe, proportionate checks;
  - `zxfer-release-docs` to audit behavior-facing docs and release surfaces; and
  - `zxfer-known-issues` to confirm and deduplicate newly discovered risks.
- Parallelize independent reads or checks when useful. Use subagents only for a
  substantial task with clearly independent workstreams, especially read-heavy
  exploration, portability review, test-gap analysis, or documentation audit.
  Give each subagent a bounded deliverable; keep overlapping edits under one
  owner; and have the main agent synthesize findings and validate the combined
  result. Do not delegate small, serial, or tightly coupled work.
- When one result determines the next action, work sequentially. If a read or
  search returns empty, partial, or suspiciously narrow results, try a small
  number of meaningful fallbacks before concluding that evidence is absent.

## Repository and Shell Conventions

- The primary shell is `/bin/sh`. Use POSIX features only. Gate Bash-specific,
  GNU-only, or platform-specific behavior with explicit capability checks.
- Treat [`docs/coding-style.md`](docs/coding-style.md) as the style authority and
  respect `.editorconfig`: shell sources use tabs; documentation and workflow
  files retain their established space indentation and line endings.
- Keep `src/` flat and grouped by stable concern. Extend the appropriate module
  instead of adding generic `common`, `globals`, `utils`, or `lib` files.
  [`src/zxfer_modules.sh`](src/zxfer_modules.sh) is the single source-order
  authority for the launcher, partial-load validation, and direct-sourcing
  tests.
- Before reimplementing a guardrail, inspect the concern-specific modules in
  `src/`, especially reporting, execution, dependency resolution, path
  security, locking, runtime lifecycle, secure staging, error logging, remote
  hosts, snapshot state, property policy/state, and backup storage/metadata.
- Major source modules need a short `Module contract` header covering owned and
  read globals, cache mutation, and stdout returns. Top-level functions use the
  structured `Purpose:` and `Usage:` comments plus `Returns:` or `Side effects:`
  when the contract is not obvious. Update comments when contracts change.
- Shared helpers use `zxfer_`; global state uses `g_`; parsed options use
  `g_option_*`; function-scoped temporaries use `l_`; operator-facing
  environment variables use `ZXFER_*`.
- Prefer small functions, early guards, short pipelines, and comments that
  explain non-obvious `awk`, `sed`, `comm`, GNU parallel, SSH, or quoting logic.
  Keep source-time side effects minimal; runtime setup belongs in explicit
  initialization flows.

## Safety and Security Invariants

- Treat flags, positional arguments, environment variables, help text, exit
  codes, stdout/stderr formats, structured failure reports, snapshot retention,
  deletion order, rollback, and replication semantics as public interfaces.
  Change them intentionally, document compatibility impact, and add regression
  coverage.
- Fail closed when state or command success is uncertain. Check exit statuses,
  preserve the original meaningful status where required, and route
  operator-facing failures through centralized reporting helpers such as
  `zxfer_throw_error*` so structured stderr reports and `ZXFER_ERROR_LOG`
  mirroring remain coherent.
- Preserve argument boundaries and quoting. Reuse the execution, dependency,
  path-security, staging, and remote-command helpers instead of adding ad hoc
  `eval`, helper lookup, shell interpolation, or command construction.
- Scrub CLI values, environment variables, host specs, and dataset names.
  Preserve wrapper-style remote specs such as `user@host pfexec` and
  `user@host doas`; do not flatten them or reintroduce injection surfaces.
- Preserve SSH control-socket reuse and least-privilege behavior. Do not leak
  keys, credentials, rendered secrets, or sensitive command arguments to logs,
  temp files, or diagnostics.
- Preserve trusted helper lookup and `ZXFER_SECURE_PATH` /
  `ZXFER_SECURE_PATH_APPEND` behavior locally and over `-O` / `-T`. Feature-test
  optional dependencies and platform-varying tool flags.
- Allocate temporary files, FIFOs, and private directories through the per-run
  0700 temp-root and secure-staging helpers. Do not add redundant per-file
  cleanup when trap-exit owns the root, and do not weaken ownership, symlink,
  hard-link, permission, or atomic-publication checks.
- Never add telemetry, network endpoints, raw-device integration paths, or new
  `sudo`/privilege requirements without an explicit design and user approval.

## Documentation and Test Alignment

- Add or update focused shunit2 coverage whenever shell behavior or a public
  helper changes. Reuse `tests/test_helper.sh` before adding suite-local
  scaffolding, and keep expectations aligned with shipped behavior.
- For behavior or public-interface changes, review the relevant `README.md`,
  `CHANGELOG.txt`, man pages, `docs/`, examples, and inline help. Review
  `CONTRIBUTING.md`, `SECURITY.md`, `KNOWN_ISSUES.md`, packaging metadata,
  workflow files, and the PR template when their concerns are affected.
- Update Mermaid control-flow diagrams in `README.md` or
  `docs/architecture.md` when replication flow, lifecycle/state ownership, or
  module boundaries change.
- Document platform-specific differences explicitly and gate or annotate tests
  so FreeBSD, Linux/OpenZFS, illumos/Solaris, and OpenZFS-on-macOS expectations
  remain visible.
- When changing coverage behavior, deliberately review
  `tests/coverage_policy.tsv` and `tests/coverage_baseline/bash-xtrace/`.
  Generated reports do not replace policy review.

## Validation

- Use `./tests/validate.sh quick [PATH...]` for the first host-safe feedback
  loop. It runs only offline budget and mapped unit checks while printing wider
  integration, performance, and documentation recommendations.
- During shell iteration, run the closest peer suites with
  `./tests/run_shunit_tests.sh tests/test_<area>.sh`. Before handoff for shell
  logic, tests, or validation tooling, run `./tests/validate.sh full`; it is the
  discoverable front door for the pinned lint stack, full shunit2 suite, and
  enforced bash-xtrace coverage.
- For documentation-only changes, prefer `git diff --check`, link/command
  inspection, and rendered-structure review. Use `./tests/validate.sh docs` when
  spelling, workflow, or budget checks are relevant; it may populate the pinned
  lint-tool cache.
- Automated agents must never invoke `tests/run_integration_zxfer.sh` directly
  on the host, including with `--yes`. When end-to-end ZFS coverage is warranted,
  use `tests/run_vm_matrix.sh` or `./tests/validate.sh vm` only with disposable
  guests and host-friendly `smoke` or `local` profiles. Broader `full` / `ci`
  profiles and slow emulated guests remain human-run only; do not execute them
  automatically.
- The integration harness must remain file-backed. Never target live pools,
  datasets, raw devices, loopback devices, or host import/export paths without
  explicit user confirmation. For a zero-host-risk requirement, recommend a
  disposable VM rather than claiming the harness is fully sandboxed.
- Performance work requires a representative baseline and explicit resource
  limits. Keep throughput tests manual/non-gating unless the user requests the
  documented disposable-guest path; report timing and resource trade-offs.
- If a required check cannot run because tooling, permissions, platform, or
  time is unavailable, state it as not run, explain why, and give the safest
  exact follow-up command.

## Review Mode

- Lead with actionable findings ordered by severity and grounded in file and
  line references.
- Prioritize data safety, security, replication correctness, failure handling,
  POSIX/platform portability, missing tests, public-interface drift, and docs
  drift over cosmetic style.
- Avoid low-signal findings unless they materially affect maintainability,
  operator clarity, or one of the priorities above.
- If no issues are found, say so directly, then list unverified areas and
  residual risk.

## Material Context to Resolve

Before risky or behavior-changing work, determine the target platforms, whether
any validation may create disposable pools, the expected user-visible behavior
and compatibility constraints, packaging/CI/release impact, and any performance
limits. Look for these answers in the task and repository first. Ask the user
only when the information is unavailable and a reasonable assumption could
change the result or blast radius.
