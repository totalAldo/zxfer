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
