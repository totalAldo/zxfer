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
- `tests/`: shunit2 suites, coverage runner, the stable integration entry
  point plus concern fragments, and the VM-backed integration matrix
- `docs/`: operator and contributor guides
- `examples/`: runnable command templates for common workflows
- `man/`: primary CLI reference (`zxfer.8`, `zxfer.1m`)
- `packaging/`: packaging-specific assets such as the RPM spec and plaintext README
- `.github/`: workflows, templates, and `CODEOWNERS`

## Required Validation

The profile dispatcher provides one discoverable front door for the existing
validation entrypoints:

```sh
./tests/validate.sh --list
./tests/validate.sh full
```

`full` runs the complete host-safe lint, unit, and enforced bash-xtrace
coverage stack. Profile composition lives in `tests/validation_profiles.tsv`;
`tests/validation_map.tsv` maps changed path patterns to unit suites plus
recommended integration groups, performance cases, and documentation
surfaces. Neither file is evaluated as shell code. `quick` executes only the
offline budget and mapped unit checks; `vm` accepts only `smoke` or `local`.
No profile invokes the direct host integration harness.
`quick` and `full` run independent suites with four workers by default; set
`ZXFER_VALIDATE_JOBS` to another positive integer for a constrained host.

Run unit tests:

```sh
./tests/run_shunit_tests.sh
```

Run the pinned local lint stack:

```sh
./tests/run_lint.sh
```

The lint stack includes the complexity and anti-rebloat budget gate
(`./tests/run_lint.sh budget`), which enforces universal per-module,
per-function, focused-test, integration-fragment, integration-runner, and
shunit `setUp` ceilings plus sensitive-caller ratchets from
`tests/budget_policy.tsv`.
It also checks that `man/zxfer.1m` is the exact generated Solaris/illumos
rendering of canonical `man/zxfer.8`; edit only the `.8` page, then run
`./tests/generate_solaris_manpage.sh --write`.
The budget is also an explicit GitHub Actions lint-matrix target, and a
workflow contract test keeps the local runner target list and CI matrix in
sync. Dependency-free targets such as `budget` and `--list` do not initialize
or download the pinned lint toolchain.
Lowering a ceiling or caller ratchet is routine maintenance; raising one
requires explicit justification in the PR that edits it. Use
`./tests/run_budget_check.sh --list` to print current measured values in
policy format when ratcheting budgets down.

For optional, non-gating evidence about the changed-code loop, record warmed
named-test and representative quick-validation timings without applying a
threshold:

```sh
./tests/run_dx_benchmark.sh \
  --case named,quick --samples 5 \
  --output-dir /tmp/zxfer-dx-candidate
```

The complete `shunit` and `validate` timing cases are available for wider
measurements; see [docs/testing.md](./docs/testing.md).

The shell lint targets include tracked and non-ignored untracked `*.sh` files
and the `zxfer` launcher, so a newly extracted module is checked before it is
staged. Ignored files remain outside the lint source set.

If you prefer a prebuilt contributor environment, open the repository in the
included `.devcontainer/` from GitHub Codespaces or VS Code. It preinstalls
the same pinned multi-shell, lint, and `kcov` tooling used for local lint,
shunit2, and coverage work on its Ubuntu 24.04 base, but it does not replace
a ZFS-capable host, disposable VM, or QEMU-capable host for the integration
runners.

Run targeted suites when editing a specific area:

```sh
./tests/run_shunit_tests.sh tests/test_zxfer_replication.sh
```

List suites or the named tests in one suite, then run only the needed tests:

```sh
./tests/run_shunit_tests.sh --list
./tests/run_shunit_tests.sh --list-suites
./tests/run_shunit_tests.sh --list-tests tests/test_zxfer_replication.sh
./tests/run_shunit_tests.sh \
  --suite tests/test_zxfer_replication.sh --test test_name \
  --suite tests/test_zxfer_exec.sh --test another_test_name
```

Named tests are validated as a batch before any selected suite starts. A
repeated suite is merged into its first position and executes once with all of
its selected tests.

Run coverage when useful:

```sh
./tests/run_coverage.sh
```

Coverage runs are report-only by default. Targeted bash-xtrace suite runs stay
report-only because a partial trace cannot satisfy the full-tree policy. Only
a full run with `--enforce` applies the repository thresholds.

Run the enforced bash-xtrace coverage gate when changing shell logic, tests,
or coverage tooling:

```sh
ZXFER_COVERAGE_MODE=bash-xtrace ./tests/run_coverage.sh --enforce
```

That local run matches the GitHub Actions policy lane: it checks the committed
minimums in `tests/coverage_policy.tsv`, rejects regressions relative to
`tests/coverage_baseline/bash-xtrace/summary.tsv`, and writes the
`missing.txt` diff that CI publishes in the PR step summary.

Run the default unattended VM-backed integration profile:

```sh
./tests/run_vm_matrix.sh --profile local
```

Run guest shunit2 on the same disposable VM boundary when a change needs
end-to-end shell validation under the guest OS rather than only on the host:

```sh
./tests/run_vm_matrix.sh --profile local --test-layer shunit2
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

Integration test bodies live in concern-focused files under
`tests/integration/`. `tests/integration_fragment_manifest.tsv` is the fixed,
non-evaluated source order, while `tests/integration_test_registry.tsv` is the
exact execution order and pre-pool classification. Add a case to the matching
fragment and registry row; change the fragment manifest only when adding or
removing a whole concern fragment. The stable runner keeps ownership of
argument parsing, confirmation, pool lifecycle, filtering, supervision, and
cleanup.

## Documentation Expectations

When behavior changes, update the relevant docs:

- `README.md`
- `CHANGELOG.txt`
- canonical `man/zxfer.8` (regenerate `man/zxfer.1m` rather than editing it)
- `docs/` guides when workflows or platform behavior changes
- `SECURITY.md` when trust boundaries, helper resolution, or failure-report
  handling change
- `KNOWN_ISSUES.md` if the change resolves or introduces a real open issue
- `examples/README.md` when runnable wrappers or sample workflows change
- `packaging/README.txt` and related packaging metadata when install, helper,
  or dependency expectations move
- relevant `.github/` workflow or template files when validation entrypoints,
  required checks, or contributor expectations change
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
`bash --posix`, and `busybox ash` on every push, plus a non-blocking `posh`
lane on pushes to `main`, and a separate non-blocking Docker-backed `kcov`
coverage artifact job. Local development does not require `kcov`, but shell-
portability-sensitive changes should mention whether those CI lanes were
considered.
