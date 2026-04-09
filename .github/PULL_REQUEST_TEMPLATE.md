## Summary

Describe the change and why it is needed.
If this change touches CI, coverage policy, or coverage baselines, explain that
here.

## Validation

- [ ] `./tests/run_lint.sh`
- [ ] `./tests/run_shunit_tests.sh`
- [ ] `ZXFER_COVERAGE_MODE=bash-xtrace ./tests/run_coverage.sh` when shell logic, tests, or coverage tooling changed
- [ ] targeted suites for edited modules
- [ ] integration tests, if safe and relevant
- [ ] GitHub Actions test matrix passes (including FreeBSD and OmniOS/illumos VMs)
- [ ] docs, workflow metadata, and coverage policy/baseline files updated as needed

## Platforms Considered

- [ ] FreeBSD
- [ ] Linux
- [ ] illumos / Solaris
- [ ] OpenZFS on macOS

## CI / Coverage Notes

Call out any intentional changes to:

- pinned lint tooling or workflow behavior
- bash-xtrace coverage policy or `tests/coverage_baseline/bash-xtrace/`
- portable-shell expectations (`dash`, `bash --posix`, `busybox ash`, `posh`)

## Safety / Security Notes

Call out any impact on:

- snapshot deletion
- rollback behavior
- remote command execution
- backup metadata
- secure-PATH assumptions
