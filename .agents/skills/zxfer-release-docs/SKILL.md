---
name: zxfer-release-docs
description: Find and plan documentation updates for zxfer behavior changes, public interfaces, release notes, man pages, examples, packaging, CI, and diagrams. Use when a change affects CLI flags, environment variables, defaults, errors, workflows, commands, platform behavior, packaging, tests, release artifacts, or user-facing docs.
---

# zxfer Release Docs

## Workflow

1. Inspect the diff and identify changed public behavior, commands, defaults, dependencies, validation flow, platform expectations, or operator-visible output.
2. Check the relevant docs surface:
   - `README.md`
   - `CHANGELOG.txt`
   - `CONTRIBUTING.md`
   - `SECURITY.md`
   - `KNOWN_ISSUES.md`
   - `docs/`
   - `examples/README.md`
   - `packaging/README.txt`
   - `man/`
   - `.github/`
3. If replication logic, state initialization, or feature flow changes, check Mermaid diagrams in `docs/architecture.md` and `README.md`.
4. If commands, dependencies, installed paths, test entry points, packaging, or release expectations change, check related packaging and workflow files.
5. If no docs update is needed, state why in the final summary.

## Update Guidance

- Keep docs aligned with shipped behavior, not planned behavior.
- Mention affected platforms explicitly when behavior differs by OpenZFS variant or OS.
- Keep examples safe and avoid live-pool commands unless clearly marked and justified.
- Do not add a changelog entry for purely internal agent-instruction or docs-only maintenance unless the user asks.
