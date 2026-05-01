# Roadmap

Last reviewed: 2026-04-29

This document tracks the main feature additions, refactors, and compatibility
decisions that remain after the current reliability and maintainability work.
It is directional, not a release promise, and should be revisited whenever the
supported-platform set or upstream OpenZFS support policy changes.

## Planning Principles

- Safety, data integrity, and transparent failure handling remain higher
  priority than throughput work.
- Compatibility changes must be deliberate, documented, and reflected in
  tests, docs, and man pages before they ship.
- zxfer will not preserve compatibility indefinitely for end-of-life operating
  systems or unmaintained upstream OpenZFS release lines.
- As supported operating systems age out of vendor or project support, the
  minimum OpenZFS floor required by zxfer will rise accordingly.

## Platform And Version Floor

- Keep the minimum supported OpenZFS generation on the project-wide 2.0+
  baseline shared by currently supported Linux, FreeBSD, OmniOS/illumos, and
  OpenZFS-on-macOS workflows.
- For FreeBSD, follow maintained upstream branches. For releases published
  after 2026-05-01, the supported FreeBSD baseline is 14.x and 15.x; FreeBSD
  13.x, stable/13, and older end-of-life branches are not guaranteed.
- Prefer actively maintained release lines rather than historical version
  compatibility for its own sake; do not add transition paths for pre-2.0
  OpenZFS behavior.
- Stop carrying compatibility paths whose only purpose is to support operating
  systems or OpenZFS release lines that have already reached end of support.
- For illumos and OmniOS, define the effective compatibility floor by the
  oldest currently supported OmniOS train instead of preserving behavior for
  ended trains. As of 2026-04-11, the official OmniOS release schedule shows
  `r151054` as the supported LTS train through 2028-05-01 and `r151056` as
  the current supported stable train through 2026-11-02.
- When the floor rises, follow through by removing old property, send/receive,
  and dependency-compatibility branches that only exist for legacy behavior.

## Data-Path Features

- Add `mbuffer` support. The preferred shape is passive capability detection
  with a predictable fallback, plus an explicit switch so operators can force
  enablement or force it off when tuning or debugging a replication path.
- Add send and receive resume-token support so interrupted runs can continue
  from `receive_resume_token` state instead of restarting large transfers from
  scratch.
- Investigate support for modern OpenZFS send and receive options such as
  `-L` and `-c`, including their interaction with raw sends, compression,
  remote capability detection, and operator-visible defaults.
- Decide which of the newer data-path options should stay implicit after
  capability detection and which deserve explicit CLI controls because they can
  affect interoperability, debugging, or throughput tuning.

## Property Logic Follow-Ups

- Keep property discovery, filtering, backup, and reconciliation aligned with
  the OpenZFS 2.0+ support floor; do not reintroduce transition paths for
  older unsupported behavior.
- Preserve the v2 backup metadata model as the current fail-closed schema:
  source-root-relative rows under explicit source and destination roots, with
  no v1 or mountpoint-local fallback probes.
- Continue tightening focused tests around inheritance, ignore lists,
  dataset-type-specific `-U` skip logic, post-seed reconciliation, and
  cross-platform property differences as supported OpenZFS behavior changes.

## Performance Validation

- Keep the new `tests/run_perf_tests.sh` harness informative first: baseline
  comparisons should warn on throughput, startup, or cleanup regressions
  without becoming a hard CI gate.
- Expand fixtures deliberately when a change needs them, such as `mbuffer`
  experiments, send-option variants, larger compression cases, or additional
  concurrency/property-logic profiles.
- Preserve VM-backed optional execution through
  `tests/run_vm_matrix.sh --test-layer perf` so agents and contributors can
  collect comparable measurements inside disposable guests.
- Promote a subset to automation only after the fixtures are stable enough to
  avoid noisy host-specific failures.

## Known-Issue Burn-Down

- Keep [../KNOWN_ISSUES.md](../KNOWN_ISSUES.md) reserved for concrete open
  failures that still affect current releases, and move resolved remediation
  themes back into changelog, architecture, or testing documentation.
- The earlier architectural backlog around lock and lease lifecycle handling,
  PID ownership validation, exact-status propagation, and literal token
  parsing has largely been retired on the current branch; prefer follow-on
  work that removes newly confirmed platform-specific gaps and behavior
  exceptions instead of reopening those resolved classes.
- Prefer centralized fixes that retire classes of issues over one-off patches
  at individual call sites.

## Upstream Lifecycle References

- [OpenZFS release policy (`RELEASES.md`)](https://github.com/openzfs/zfs/blob/master/RELEASES.md):
  official branch-maintenance policy for current versus LTS lines. OpenZFS
  does not currently publish a separate standalone end-of-life calendar; this
  release-policy document is the upstream lifecycle reference zxfer should use.
- [OpenZFS repository README](https://github.com/openzfs/zfs):
  current supported kernels, Linux distributions, and FreeBSD releases.
- [OpenZFS releases](https://github.com/openzfs/zfs/releases):
  current release train and release notes.
- [OpenZFS documentation](https://openzfs.github.io/openzfs-docs/index.html):
  upstream user and developer documentation.
- [FreeBSD release information](https://www.freebsd.org/releases/):
  official supported release list and links to branch end-of-life dates.
- [OmniOS release schedule](https://omnios.org/schedule.html):
  official supported-train and end-of-support dates for OmniOS.
