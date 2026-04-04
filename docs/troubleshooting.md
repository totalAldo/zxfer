# Troubleshooting

## Missing Dependency Errors

Example:

```text
Required dependency "zfs" not found in secure PATH ...
```

What it usually means:

- the required binary is outside the secure allowlist
- `ZXFER_SECURE_PATH` is too narrow
- the remote host does not have the required tool in the expected directories

What to check:

- `ZXFER_SECURE_PATH`
- `ZXFER_SECURE_PATH_APPEND`
- remote `zfs`, `ssh`, `cat`, `parallel`, or `zstd` availability

## Remote Dependency Probe Failures

Example:

```text
Failed to query dependency "zfs" on host ...
```

What it usually means:

- ssh failed before the probe completed
- the wrapped host spec is wrong
- the remote shell could not run the secure-PATH probe command

What to inspect:

- direct `ssh` connectivity
- wrapper commands such as `pfexec` / `doas`
- the stderr failure report and `last_command`

## Snapshot Discovery Failures

Example:

```text
Failed to retrieve snapshots from the source
```

What it usually means:

- remote `zfs list` failed
- remote `parallel` or `zstd` was missing or misresolved
- source dataset naming or quoting was wrong on the remote side

What to inspect:

- stderr failure report
- source-side snapshot listing path
- any shell quoting problems on the remote host

## Backup Metadata Restore Failures

Example:

```text
Cannot find backup property file
```

Or:

```text
... backup metadata file is not owned by root or the effective UID ...
```

What it usually means:

- `ZXFER_BACKUP_DIR` does not contain the expected file
- secure file ownership / mode checks rejected the metadata
- legacy fallback was needed but not present

What to inspect:

- `ZXFER_BACKUP_DIR`
- ownership and permissions of `.zxfer_backup_info.*`
- whether the backup belongs to the intended dataset

## Failure Report Logging And Email Alerts

To persist every non-zero zxfer failure report, set `ZXFER_ERROR_LOG` to an
absolute path whose parent directory already exists:

```sh
mkdir -p /var/log/zxfer
chmod 700 /var/log/zxfer

ZXFER_ERROR_LOG=/var/log/zxfer/error.log \
./zxfer -v -R tank/src backup/dst
```

The log file receives the same structured block that zxfer writes to `stderr`:

```text
zxfer: failure report begin
failure_class: runtime
failure_stage: send/receive
source_root: tank/src
destination_root: backup/dst
last_command: '/sbin/zfs' 'send' ...
zxfer: failure report end
```

To extract the newest report from an existing log:

```sh
awk '
/^zxfer: failure report begin$/ { block=$0 ORS; capture=1; next }
capture {
	block = block $0 ORS
	if ($0 ~ /^zxfer: failure report end$/) {
		last = block
		block = ""
		capture = 0
	}
}
END { printf "%s", last }
' /var/log/zxfer/error.log
```

To extract a single field from that block:

```sh
latest_report=$(awk '
/^zxfer: failure report begin$/ { block=$0 ORS; capture=1; next }
capture {
	block = block $0 ORS
	if ($0 ~ /^zxfer: failure report end$/) {
		last = block
		block = ""
		capture = 0
	}
}
END { printf "%s", last }
' /var/log/zxfer/error.log)

printf '%s\n' "$latest_report" | awk -F': ' '/^failure_stage: / { print $2; exit }'
```

For a complete wrapper that runs zxfer, captures the current run's structured
failure report from stderr, and sends mail through `mailx`, BSD `mail`, or
`sendmail`, see
[examples/error-log-email-notify.sh](../examples/error-log-email-notify.sh). The
example auto-detects those mailers, accepts `MAIL_FROM`,
`MAIL_FROM_FLAG`, and `SENDMAIL_FROM_FLAG` overrides for sender-address
requirements, and can be validated with
`sh ./examples/error-log-email-notify.sh --self-test` before you point it at a
real pool or MTA.

## Integration Harness Failures

Examples:

- `permission denied` from `zpool create`
- snapshot visibility assertions timing out on macOS/OpenZFS

What it usually means:

- the current user cannot create file-backed pools
- the local platform reports snapshot state more slowly than the harness expects
- a failing test may be exposing a real receive-path bug rather than a harness problem

What to inspect:

- test-specific output block
- visible snapshots listed in the assertion failure
- host privileges for `zpool` / `zfs`
