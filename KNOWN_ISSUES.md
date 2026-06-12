# KNOWN ISSUES

This file tracks open issues that still matter for current releases. Issues are
ordered by remediation priority: exploitable security flaws and destructive
correctness bugs first, then reliability and interface drift, then lower-risk
documentation and portability gaps.

Generic architecture notes are intentionally omitted unless they currently
describe a concrete failure mode or exploit path.

File references below use the current flat `src/` layout and the shared
`src/zxfer_modules.sh` loader. Some support modules are still covered inside
adjacent shunit suites, so a referenced test file may not always be
peer-named to the implementation module it exercises.

## Correctness And Portability

No open issues are currently tracked in this section.

### Resolved: silent destroy/rollback/resend churn on GUID-diverged destinations (fixed 2026-06-12)

Before 2026-06-12, when destination snapshots matched source snapshots by
NAME but carried different GUIDs (diverged data under identical names), a
`-d` run silently destroyed those destination snapshots, rolled the
destination back to the last GUID-matching common snapshot, and re-sent the
whole range — on every run, with no operator messaging. The legacy name-only
matching variant of the same fixture was silent in the opposite direction: it
reported "No new snapshots to transfer" and treated diverged data as in sync.

This is resolved by the divergence contract (see `README.md` and the `-d`/`-F`
entries in `man/zxfer.8`): an always-on stderr warning names the diverged
dataset, the count, and example snapshots with both GUIDs; destructive
convergence requires BOTH `-d` and `-F` (otherwise the run fails closed with
zero actions for the diverged dataset); and a post-receive verification of the
live destination listing turns any re-divergence into a structured error
naming the snapshot. Regression coverage:
`tests/test_zxfer_planning_blackbox.sh` (divergence contract pins) and
`tests/test_zxfer_snapshot_reconcile.sh` (classifier, gate, and verification
units).
