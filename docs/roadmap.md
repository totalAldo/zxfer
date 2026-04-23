# Roadmap

Last reviewed: 2026-04-21

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

- Revisit the minimum supported OpenZFS generation and move the project to a
  modern baseline shared by currently supported platforms, with an OpenZFS
  2.3+ feature floor as the current working target for Linux, FreeBSD, and
  OpenZFS on macOS, pending validation against the oldest still-supported
  platform in the matrix.
- Confirm the final floor against the upstream OpenZFS release policy before
  enforcement. OpenZFS currently maintains one LTS branch and one current
  branch, so zxfer should prefer actively maintained release lines rather than
  historical version compatibility for its own sake.
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

## Property Logic Refactor

- Refactor property discovery, filtering, backup, and reconciliation around the
  modern OpenZFS floor instead of keeping branches that only exist for older
  unsupported behavior.
- Simplify unsupported-property handling once older platform and OpenZFS
  combinations are intentionally dropped.
- Revisit the property backup and restore flow so the implementation model is
  easier to reason about and less dependent on compatibility-era exceptions.
- Add or tighten focused tests around inheritance, ignore lists, skip logic,
  and cross-platform property differences after the refactor lands.

## Performance Validation

- Add repeatable performance tests that can detect throughput regressions and
  quantify the cost or benefit of changes such as `mbuffer`, send options,
  concurrency tuning, and property-logic refactors.
- Define a small set of representative fixtures: large snapshot chains, many
  sibling datasets, local runs, remote runs, and compression-enabled runs.
- Capture both wall-clock and behavioral metrics such as startup latency,
  replication throughput, cleanup time, and remote round-trip counts where
  practical.
- Keep performance testing informative first. It does not need to become a
  hard CI gate immediately, but it should be reliable enough to support manual
  regression checks and future automation.

## Known-Issue Burn-Down

- Work through the remaining items in [../KNOWN_ISSUES.md](../KNOWN_ISSUES.md)
  as planned engineering work rather than leaving them as indefinite backlog.
- Prioritize the concrete remaining issues already tracked there: the
  OpenZFS-on-macOS property-reconciliation gap, remote `-O ... -j > 1`
  upfront GNU Parallel validation, and the recursive `-o` inheritance
  flattening behavior.
- The earlier architectural backlog around lock and lease lifecycle handling,
  PID ownership validation, exact-status propagation, and literal token
  parsing has largely been retired on the current branch; prefer follow-on
  work that removes the remaining platform-specific gaps and behavior
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
- [OmniOS release schedule](https://omnios.org/schedule.html):
  official supported-train and end-of-support dates for OmniOS.
