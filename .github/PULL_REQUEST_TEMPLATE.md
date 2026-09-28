## Summary

Describe the change and why it is needed.
If this change touches CI or coverage tooling, explain that here.

## Validation

- [ ] `./tests/validate.sh full` (or the equivalent focused commands below)
- [ ] `./tests/run_lint.sh`
- [ ] `./tests/run_shunit_tests.sh`
- [ ] `ZXFER_COVERAGE_MODE=bash-xtrace ./tests/run_coverage.sh` when shell logic, tests, or coverage tooling changed
- [ ] `./tests/validate.sh quick` for the edited paths (their entry suites plus the contract suites)
- [ ] integration tests, if safe and relevant
- [ ] `./tests/run_microbench.sh` and `./tests/run_perf_ab.sh --baseline-ref main` (host-safe) when performance-sensitive behavior changed; `./tests/run_vm_matrix.sh --test-layer perf` / `perf-compare` for real pools
- [ ] GitHub Actions test matrix passes (including FreeBSD and OmniOS/illumos VMs)
- [ ] docs and workflow metadata updated as needed

## Platforms Considered

- [ ] FreeBSD
- [ ] Linux
- [ ] illumos / Solaris
- [ ] OpenZFS on macOS

## CI / Coverage Notes

Call out any intentional changes to:

- pinned lint tooling or workflow behavior
- report-only bash-xtrace coverage output or the coverage workflow
- portable-shell expectations (`dash`, `bash --posix`, `busybox ash`, `posh`)
- spawn budgets in `tests/perf_budgets.tsv` (they only go down), the advisory
  wall-clock A/B, or VM-backed perf artifacts; wall-clock perf is informative
  and not a required GitHub Actions gate

## Safety / Security Notes

Call out any impact on:

- snapshot deletion
- rollback behavior
- remote command execution
- backup metadata
- secure-PATH assumptions
