---
name: zxfer-pr-review
description: Review zxfer changes and pull requests for safety, security, ZFS replication correctness, shell portability, missing tests, docs drift, and operator-visible behavior. Use when Codex is asked to review a zxfer diff, PR, commit, branch, or uncommitted changes, or to prepare review feedback before merging.
---

# zxfer PR Review

## Workflow

1. Load `AGENTS.md`, `docs/coding-style.md`, and the changed diff before judging the change.
2. Inspect scope with repo-local evidence: changed files, nearby source, matching `tests/test_*.sh`, relevant docs, and public interfaces touched by the diff.
3. Read `references/review-checklist.md` when the change touches replication, remote execution, path security, runtime cleanup, reporting, platform behavior, tests, or docs.
4. Review as a blocker-finding pass, not a style pass. Do not implement fixes unless the user asks for implementation.
5. If the diff is too large to review completely, state the reviewed scope and the unreviewed residual risk.

## Output

- Lead with findings ordered by severity.
- Include tight file and line references for every finding.
- Prioritize data-loss risk, shell injection, broken replication ordering, incompatible CLI/output changes, missing validation, and stale docs.
- Avoid low-signal style comments unless they materially affect maintainability, portability, safety, or operator clarity.
- If no issues are found, say that directly and still list test gaps or areas not verified.
