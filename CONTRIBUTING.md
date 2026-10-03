# Contributing

## Start Here

Read the [developer walkthrough](./docs/developer-walkthrough.md) to follow one
replication operation, then the [data formats guide](./docs/data-formats.md) to
understand the records passed between modules. Keep
[architecture.md](./docs/architecture.md) nearby as the ownership reference;
you do not need to read every module before making a focused contribution.

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
- `tests/`: shunit2 module and contract suites, the `validate.sh` front
  door, the coverage runner, the argv fuzz and performance tools, the stable
  integration entry point plus concern fragments, and the VM-backed
  integration matrix
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

`full` runs the complete host-safe lint, unit, and report-only bash-xtrace
coverage stack. Profile composition lives in `tests/validation_profiles.tsv`.
For a changed `src/zxfer_NAME.sh`, `quick` runs `tests/test_zxfer_NAME.sh` and
every black-box `tests/test_contract_*.sh` suite, selected by name;
`tests/validation_map.tsv` adds the exceptions plus recommended integration
groups, performance cases, and documentation surfaces. Neither file is
evaluated as shell code. `quick` executes only the offline budget and unit
checks; `vm` accepts only `smoke` or `local`.
No profile invokes the direct host integration harness.
`quick` and the unit step of `full` run independent suites with four workers
by default; set `ZXFER_VALIDATE_JOBS` to another positive integer for a
constrained host. The coverage step of `full` reruns every suite at the unit
runner's default (the CPU count, at most 4), which `ZXFER_VALIDATE_JOBS` does
not change.

Run unit tests:

```sh
./tests/run_shunit_tests.sh
```

Run the pinned local lint stack:

```sh
./tests/run_lint.sh
```

The lint stack includes the anti-rebloat budget gate
(`./tests/run_lint.sh budget`), which checks only the sensitive-caller
ratchets in `tests/budget_policy.tsv` (production `eval`, `$(date` and
`mktemp` call sites, plus a `zxfer_profile_now_ms` row held at 0 so the
removed forking clock helper cannot return); any other record kind fails
the gate. There are no module, function, or test-file size ceilings and
no machine-checked layering or ownership policy; collapsing modules and
inlining single-caller wrappers is welcome.
It also checks that `man/zxfer.1m` is the exact generated Solaris/illumos
rendering of canonical `man/zxfer.8`; edit only the `.8` page, then run
`./tests/generate_solaris_manpage.sh --write`.
The budget is also an explicit GitHub Actions lint-matrix target, and a
workflow contract test keeps the local runner target list and CI matrix in
sync. Dependency-free targets such as `budget` and `--list` do not initialize
or download the pinned lint toolchain.
Lowering a caller ratchet is routine maintenance; raising one requires
explicit justification in the PR that edits it. Use
`./tests/run_budget_check.sh --list` to print current measured values in
policy format when ratcheting budgets down.

The shell lint targets include tracked and non-ignored untracked `*.sh` files
and the `zxfer` launcher, so a newly extracted module is checked before it is
staged. Ignored files remain outside the lint source set.

If you prefer a prebuilt contributor environment, open the repository in the
included `.devcontainer/` from GitHub Codespaces or VS Code. It preinstalls
the same pinned multi-shell, lint, and `kcov` tooling used for local lint,
shunit2, and coverage work on its Ubuntu 24.04 base, but it does not replace
a ZFS-capable host, disposable VM, or QEMU-capable host for the integration
runners.

Run targeted suites when editing a specific area. The tests of
`src/zxfer_NAME.sh` live in `tests/test_zxfer_NAME.sh` and the
`tests/suites/zxfer_NAME_*_tests.sh` fragments it runs; the black-box contract
suites that drive the real launcher are `tests/test_contract_*.sh`. A new
behavior gets a contract case first; unit tests cover what the canned zfs
cannot express (`docs/testing.md`, "Where a new test goes"):

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

A suite that runs longer than 15 minutes is stopped and reported as timed out;
raise the limit with `--suite-timeout SECONDS` (or `ZXFER_TEST_SUITE_TIMEOUT`,
0 disables it) on a slow emulated guest. `--skip-tool-suites` leaves out the
self-tests of the tooling (`tests/test_run_*.sh`, `tests/test_validate.sh`,
`tests/test_ci_*.sh`, `tests/test_generate_solaris_manpage.sh`) when you only
changed `src/`. A suite that takes more than about 10 s alone belongs in
`RUNNER_SLOW_SUITES` at the top of the runner, which parallel runs start
first. See [docs/testing.md](./docs/testing.md) for the runner's reference.

Run coverage when useful:

```sh
./tests/run_coverage.sh
```

Coverage is report-only: there is no committed minimum, baseline, or
no-regression policy, and the runner's exit status reflects only whether the
selected suites passed.

Run the bash-xtrace coverage report when changing shell logic, tests, or
coverage tooling:

```sh
ZXFER_COVERAGE_MODE=bash-xtrace ./tests/run_coverage.sh
```

That local run matches the GitHub Actions coverage lane, which publishes
`summary.tsv` in the step summary and uploads the full report as an artifact.

Run the default unattended VM-backed integration profile:

```sh
./tests/run_vm_matrix.sh --profile local
```

Run guest shunit2 on the same disposable VM boundary when a change needs
end-to-end shell validation under the guest OS rather than only on the host:

```sh
./tests/run_vm_matrix.sh --profile local --test-layer shunit2
```

A change to process groups, `ls` parsing or userland flags should pass the
FreeBSD guest's unit layer before it is pushed
(`--guest freebsd --test-layer shunit2`).

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

Integration test bodies live in concern-focused `tests/integration/NAME_tests.sh`
fragments, which the harness loads in sorted order, while
`tests/integration_test_registry.tsv` is the exact execution order and
pre-pool classification. Add a case to the matching fragment and a registry
row. A fragment holds only function definitions in the shfmt layout (a new
concern is a new `NAME_tests.sh` file); the harness rejects anything else
before it touches a pool. The stable runner keeps ownership of argument
parsing, confirmation, pool lifecycle, filtering, supervision, and cleanup.

For a performance-sensitive change, compare the helper-spawn counts and the
wall clock against `main` on the canned zfs; neither needs ZFS or root:

```sh
./tests/run_microbench.sh
./tests/run_perf_ab.sh --baseline-ref main --sizes 25,100 --reps 5
```

The spawn budgets in `tests/perf_budgets.tsv` only go down: when a change
lowers a count, lower its row in the same change. For real pools, use the VM
matrix `perf` layer, or `perf-compare` to time another ref in the same guest:

```sh
ZXFER_VM_PERF_BASELINE_REF=main ./tests/run_vm_matrix.sh --profile smoke --test-layer perf-compare
```

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
  features, ensure the corresponding Mermaid diagrams in
  `docs/architecture.md` are updated to reflect the new control flow.

## Versioning

A zxfer version is `MAJOR.MINOR.YYYYMMDD`: the build date in the last field,
with MAJOR or MINOR raised only for a deliberate milestone. One string names a
build everywhere: `g_zxfer_version` in `src/zxfer_session.sh` (the
`zxfer_version` field of failure reports and the `#version` line of `-k`
backup metadata), `Version:` and the `%changelog` entry in
`packaging/zxfer.spec`, and the `v<version>` tag the spec's `Source0` fetches.
`tests/test_contract_cli_golden.sh` checks that the spec's `Version:` has this
form and that a failure report names it, and the Packaging workflow builds the
spec with `rpmbuild` on every push. This form replaced `2.0.0-YYYYMMDD` with
2.0.20260930, because `rpmbuild` rejects a `-` in `Version:`.

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
- whether CI or coverage tooling changed intentionally

GitHub Actions also runs an Ubuntu portable-shell matrix for `dash`,
`bash --posix`, and `busybox ash` on every push, and a separate non-blocking
Docker-backed `kcov` coverage artifact job. The tool self-tests run only on
the ubuntu-26.04 and macos-26 lanes. Local development does not require
`kcov`, but shell-portability-sensitive changes should mention whether those
CI lanes were considered.
