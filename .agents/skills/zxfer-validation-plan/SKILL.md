---
name: zxfer-validation-plan
description: Choose and explain safe validation commands for zxfer changes based on touched files and risk. Use when Codex needs a test plan, is about to validate a change, is deciding between targeted shunit, full shunit, lint, coverage, or VM-backed integration, or must avoid unsafe host integration runs.
---

# zxfer Validation Plan

## Workflow

1. Inspect the diff and touched files before choosing commands.
2. Run `./tests/validate.sh quick <changed paths>` first: it maps `src/zxfer_NAME.sh` to its entry suite `tests/test_zxfer_NAME.sh` (fragments in `tests/suites/zxfer_NAME_*_tests.sh`) plus every black-box `tests/test_contract_*.sh` suite, and prints wider follow-ups without running them. Iterate with `./tests/run_shunit_tests.sh <suite>`; add `ZXFER_TEST_SHELL=/bin/dash` for portability-sensitive code.
3. Before handoff for shell logic, tests, or validation tooling, run `./tests/validate.sh full` (the pinned lint stack, every unit suite, and report-only bash-xtrace coverage). Product-only changes may skip the tooling self-tests while iterating with `./tests/run_shunit_tests.sh --skip-tool-suites`.
4. For performance-sensitive changes, run `./tests/run_microbench.sh` (spawn budgets in `tests/perf_budgets.tsv` only go down) and `./tests/run_perf_ab.sh --baseline-ref main` for the scenarios touched; both are host-safe.
5. For docs-only changes, prefer `git diff --check` and manual rendered-structure review unless the docs alter commands, test entry points, or shipped behavior.
6. For coverage tooling changes, run `./tests/run_shunit_tests.sh tests/test_run_coverage.sh`; coverage is report-only, with no committed policy or baseline files.

## Integration Rules

- Never run `tests/run_integration_zxfer.sh` directly on the host as an automated agent, including with `--yes`.
- Use `tests/run_vm_matrix.sh` only when a disposable guest boundary is available and the work benefits from integration coverage.
- Automatic VM-backed runs must stay on host-friendly profiles such as `--profile smoke` or `--profile local`.
- Treat `--profile full`, `--profile ci`, and slow emulated guests as manual-only unless the user explicitly asks.
- When narrowing integration during iteration, prefer `tests/run_vm_matrix.sh --profile local --guest ... --only-test ...`.
- For changes to process groups, `ls` parsing or userland flags, also run the FreeBSD guest's unit layer: `tests/run_vm_matrix.sh --profile local --guest freebsd --test-layer shunit2`.

## Output

- State the minimal targeted commands first, then the broader pre-merge commands.
- Explain any skipped required command as "not run" with the reason.
- Include residual risk when validation cannot cover platform-specific ZFS behavior.
