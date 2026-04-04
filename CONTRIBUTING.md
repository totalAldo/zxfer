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

## Repository Layout

- `zxfer`: entry point
- `src/`: functional shell modules
- `tests/`: shunit2 suites, coverage runner, integration harness
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

Run targeted suites when editing a specific area:

```sh
./tests/run_shunit_tests.sh tests/test_zxfer_zfs_mode.sh
```

Run coverage when useful:

```sh
./tests/run_coverage.sh
```

Run integration tests only on a safe host:

```sh
./tests/integration_zxfer.sh --yes --keep-going
```

Run the integration harness interactively when you want per-command approval:

```sh
./tests/integration_zxfer.sh
```

## Documentation Expectations

When behavior changes, update the relevant docs:

- `README.md`
- `CHANGELOG.txt`
- man pages
- `docs/` guides when workflows or platform behavior changes
- `KNOWN_ISSUES.md` if the change resolves or introduces a real open issue

## Pull Requests

Good pull requests explain:

- what changed
- why it changed
- what platforms were considered
- what tests were run
- whether any safety or security assumptions changed
