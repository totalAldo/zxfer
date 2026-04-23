---
name: zxfer-validation-plan
description: Choose and explain safe validation commands for zxfer changes based on touched files and risk. Use when Codex needs a test plan, is about to validate a change, is deciding between targeted shunit, full shunit, lint, coverage, or VM-backed integration, or must avoid unsafe host integration runs.
---

# zxfer Validation Plan

## Workflow

1. Inspect the diff and touched files before choosing commands.
2. Match source modules to peer suites under `tests/test_*.sh`; use targeted suites first for iteration.
3. Escalate to the required full commands when shell logic changes:
   - `./tests/run_shunit_tests.sh`
   - `./tests/run_lint.sh`
   - `ZXFER_COVERAGE_MODE=bash-xtrace ./tests/run_coverage.sh`
4. For docs-only changes, prefer `git diff --check` and manual rendered-structure review unless the docs alter commands, test entry points, or shipped behavior.
5. For coverage tooling or policy changes, review `tests/coverage_policy.tsv` and `tests/coverage_baseline/bash-xtrace/`.

## Integration Rules

- Never run `tests/run_integration_zxfer.sh` directly on the host as an automated agent, including with `--yes`.
- Use `tests/run_vm_matrix.sh` only when a disposable guest boundary is available and the work benefits from integration coverage.
- Automatic VM-backed runs must stay on host-friendly profiles such as `--profile smoke` or `--profile local`.
- Treat `--profile full`, `--profile ci`, and slow emulated guests as manual-only unless the user explicitly asks.
- When narrowing integration during iteration, prefer `tests/run_vm_matrix.sh --profile local --guest ... --only-test ...`.

## Output

- State the minimal targeted commands first, then the broader pre-merge commands.
- Explain any skipped required command as "not run" with the reason.
- Include residual risk when validation cannot cover platform-specific ZFS behavior.
