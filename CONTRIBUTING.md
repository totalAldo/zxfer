# Contributing

## Principles

zxfer manipulates real ZFS datasets and is used in production. Contributions
should prioritize:

- safety
- security
- maintainability
- performance only after the above

## Development Constraints

- target POSIX `/bin/sh`
- avoid Bash-specific features
- avoid GNU-only assumptions unless gated
- preserve cross-platform behavior where possible
- respect `.editorconfig` when your editor supports it; shell sources use tabs
  while docs and workflow files use LF line endings with space indentation
- follow [docs/coding-style.md](./docs/coding-style.md) for project-specific
  shell, naming, module, and test conventions

## Repository Layout

- `zxfer`: entry point
- `src/`: functional shell modules
- `tests/`: shunit2 suites, coverage runner, direct integration harness, and
  the VM-backed integration matrix
- `docs/`: operator and contributor guides
- `examples/`: runnable command templates for common workflows
- `man/`: primary CLI reference (`zxfer.8`, `zxfer.1m`)
- `packaging/`: packaging-specific assets such as the RPM spec and plaintext README
- `.github/`: workflows, templates, and `CODEOWNERS`

## Required Validation

Run unit tests:

```sh
./tests/run_shunit_tests.sh
```

Run the pinned local lint stack:

```sh
./tests/run_lint.sh
```

If you prefer a prebuilt contributor environment, open the repository in the
included `.devcontainer/` from GitHub Codespaces or VS Code. It preinstalls
the Ubuntu 24.04 multi-shell, lint, and `kcov` tooling used for local lint,
shunit2, and coverage work, but it does not replace a ZFS-capable host or
disposable VM or QEMU-capable host for the integration runners.

Run targeted suites when editing a specific area:

```sh
./tests/run_shunit_tests.sh tests/test_zxfer_replication.sh
```

Run coverage when useful:

```sh
./tests/run_coverage.sh
```

Run the enforced bash-xtrace coverage gate when changing shell logic, tests,
or coverage tooling:

```sh
ZXFER_COVERAGE_MODE=bash-xtrace ./tests/run_coverage.sh
```

That local run matches the GitHub Actions policy lane: it checks the committed
minimums in `tests/coverage_policy.tsv`, rejects regressions relative to
`tests/coverage_baseline/bash-xtrace/summary.tsv`, and writes the
`missing.txt` diff that CI publishes in the PR step summary.

Run the default unattended VM-backed integration profile:

```sh
./tests/run_vm_matrix.sh --profile local
```

For tighter development loops, prefer a single guest plus a named in-guest
test selection before widening back out to the full local profile:

```sh
./tests/run_vm_matrix.sh --profile local --guest ubuntu --only-test basic_replication_test
```

Run integration tests directly on a safe host only when you intentionally want
the expert/manual harness:

```sh
./tests/run_integration_zxfer.sh --yes --keep-going
```

Run the integration harness interactively when you want per-command approval:

```sh
./tests/run_integration_zxfer.sh
```

## Documentation Expectations

When behavior changes, update the relevant docs:

- `README.md`
- `CHANGELOG.txt`
- man pages
- `docs/` guides when workflows or platform behavior changes
- `KNOWN_ISSUES.md` if the change resolves or introduces a real open issue
- When modifying replication logic, state initialization, or adding new
  features, ensure the corresponding Mermaid diagrams in `architecture.md` and
  `README.md` are updated to reflect the new control flow.

## Filing Issues

Use the GitHub issue forms for bug reports, feature requests, and
platform-compatibility findings. Include the OS release, ZFS/OpenZFS version,
shell, privilege model, pool or dataset layout, and any remote-wrapper details
needed to reproduce the problem safely.

Redact hostnames, credentials, and dataset names as needed. For security-
sensitive reports, follow `SECURITY.md` instead of opening a public issue.

## Pull Requests

Good pull requests explain:

- what changed
- why it changed
- what platforms were considered
- what tests were run
- whether any safety or security assumptions changed
- whether CI, coverage policy, or baseline artifacts changed intentionally

GitHub Actions also runs an Ubuntu portable-shell matrix for `dash`,
`bash --posix`, `busybox ash`, and a non-blocking `posh` lane, plus a separate
Docker-backed `kcov` coverage artifact job. Local development does not require
`kcov`, but shell-portability-sensitive changes should mention whether those CI
lanes were considered.
