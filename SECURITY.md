# Security

## Scope

zxfer runs privileged filesystem and replication commands and often needs
either root, delegated ZFS privileges, or privileged remote wrappers. That
makes shell quoting, remote execution, file ownership checks, and dependency
resolution security-sensitive by default.

## Current Security Model

Key protections already present in the project include:

- secure-PATH resolution for required local helpers and the main remote helper lookups (`zfs`, `cat`, and GNU `parallel`)
- structured failure reporting instead of ad hoc error handling
- safe-by-default failure-report redaction for `invocation` and `last_command`
- hardened `ZXFER_ERROR_LOG` path validation
- secured property backup metadata directories and file-permission checks
- explicit handling for wrapped remote host specs

Structured failure reports now redact `invocation` and `last_command` as
`[redacted]` by default in both `stderr` output and any `ZXFER_ERROR_LOG`
mirror, so routine logs and wrappers do not capture raw command lines. If an
operator explicitly wants verbatim command text during local debugging, they
can set `ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1`; doing so is unsafe because
wrapper arguments, hook strings, or other command-line fragments may then be
written to `stderr` and `ZXFER_ERROR_LOG`. Raw ASCII control bytes in
structured failure-report values are escaped before output so terminal and
pager control sequences cannot execute from report fields.

Current open security concerns are tracked in [KNOWN_ISSUES.md](./KNOWN_ISSUES.md).

## Reporting A Vulnerability

Please do not open a public issue for a suspected command-injection, trust-
boundary, privilege-escalation, or data-destruction vulnerability until the
maintainer has had a chance to assess it privately.

Send private vulnerability reports to:

- `zxfer@totalaldo.com`

If possible, include:

- affected command line
- platform and shell
- exact stderr output
- whether the issue is local, remote, or backup-metadata related
- minimal reproduction steps

## Security Review Hotspots

Changes in these areas should receive extra scrutiny:

- remote command construction
- any `eval` usage
- secure-PATH resolution
- property backup / restore lookup
- ssh control-socket management
- snapshot deletion and rollback behavior
