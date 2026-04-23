---
name: zxfer-known-issues
description: Identify newly discovered zxfer risks, bugs, or open compatibility gaps that should be tracked in KNOWN_ISSUES.md, deduplicate them against the existing inventory, add confirmed issues by severity, and assess whether architectural remediation themes should be added or revised to address multiple issues. Use when auditing zxfer changes, test failures, bug reports, review findings, TODOs, or code paths for new known issues.
---

# zxfer Known Issues

## Context to Load

- Read `AGENTS.md`, `KNOWN_ISSUES.md`, and the current diff or bug report before editing.
- Inspect relevant source modules, matching `tests/test_*.sh`, and user-facing docs when the candidate affects public behavior, validation flow, platform behavior, security, or replication semantics.
- For review-derived candidates, use the repo evidence behind the finding rather than copying review text unverified.
- When the candidate touches portability, release docs, or validation scope, use the matching zxfer skill alongside this one.

## Issue Qualification

- Add an issue only when it describes a concrete failure mode, exploit path, operator-visible compatibility gap, or stale validation expectation that still applies to the current tree.
- Do not add speculative risks, generic architecture preferences, style complaints, or missing-test notes unless they expose a concrete shipped behavior problem.
- Do not duplicate an existing issue. If a candidate is the same failure class as an existing entry, update that entry only when new evidence changes severity, affected scope, reproduction detail, or remediation priority.
- Prefer exact file references, function names, observed command behavior, test output, or direct helper repros over broad claims.

## Severity

- `Critical`: likely direct data loss, destructive command selection, credential exposure, or exploitable security behavior in a normal supported workflow.
- `High`: realistic destructive replication, rollback, deletion, remote execution, trust-boundary, or fail-open security risk that needs priority remediation but has narrower trigger conditions than `Critical`.
- `Medium`: silent correctness loss, stale state, wrong planning, metadata corruption, platform divergence, or fail-closed behavior that can break supported workflows.
- `Low`: diagnostics, documentation drift, low-likelihood compatibility gaps, fail-closed helper validation, or limited operator-experience issues.

Keep issues ordered by remediation priority within their section: `Critical`, then `High`, then `Medium`, then `Low`. Preserve the existing heading style:

```markdown
### Medium: concise issue title

`src/file.sh` describes the exact condition and impact...
```

## Updating `KNOWN_ISSUES.md`

1. Re-read the current `KNOWN_ISSUES.md` inventory and identify the best section for each confirmed issue.
2. Insert each issue at the correct severity position, using concise prose that states:
   - where the behavior lives,
   - what condition triggers it,
   - what zxfer does incorrectly,
   - why the result matters to operators.
3. Include repro evidence when available, but keep the entry short enough to remain a tracking inventory, not a full incident report.
4. Preserve existing wording and ordering for unrelated entries.
5. If no new issue qualifies, do not edit `KNOWN_ISSUES.md`; state why the candidates were rejected or already covered.

## Architectural Remediation Pass

After adding or updating issues, review the full issue inventory, including pre-existing entries, for repeated root causes.

- Add or revise an `Architectural Remediation Themes` suggestion only when one design change would address multiple concrete issues.
- Tie each theme to the shared failure class and representative issue titles or modules.
- Keep suggestions implementation-oriented but not over-specified; describe the architectural direction, invariants it should enforce, and the issue cluster it would reduce.
- Do not add generic architecture notes for a single issue or vague maintainability concern.
- If the new issue is already covered by an existing theme, update that theme only when the new evidence changes the theme's scope or design constraint.

## Validation and Output

- For docs-only inventory updates, run `git diff --check` and manually review heading order with `rg -n '^(##|###) ' KNOWN_ISSUES.md`.
- If code or tests were changed in the same task, use `zxfer-validation-plan` to select the appropriate targeted and full validation commands.
- In the final response, list added or updated issue titles with severity, architectural themes added or revised, validation run, and residual risk or unverified evidence.
