# Troubleshooting

## Missing Dependency Errors

Example:

```text
Required dependency "zfs" not found in secure PATH ...
```

What it usually means:

- the required binary is outside the secure allowlist
- `ZXFER_SECURE_PATH` is too narrow
- the live runtime `PATH` is also confined to that same allowlist
- the remote host does not have the required tool in the expected directories

What to check:

- `ZXFER_SECURE_PATH`
- `ZXFER_SECURE_PATH_APPEND`
- every trusted directory needed for later bare helper invocations
- remote `zfs`, `ssh`, `cat`, `parallel`, or `zstd` availability

## Remote Dependency Probe Failures

Example:

```text
Failed to query dependency "zfs" on host ...
```

What it usually means:

- ssh failed before the probe completed
- the per-host remote-capability bootstrap failed before zxfer could reuse a
  cached helper-path record
- the wrapped host spec is wrong
- the remote shell could not run the secure-PATH probe command

What to inspect:

- direct `ssh` connectivity
- wrapper commands such as `pfexec` / `doas`
- whether `-V` prints a `Running remote probe [...]` line that identifies the
  exact probe command that stalled or failed before capture redirection began
- whether the target host key is already trusted in the active known-hosts
  files, because zxfer-managed ssh now defaults to `BatchMode=yes` plus
  `StrictHostKeyChecking=yes`
- whether `ZXFER_SSH_USER_KNOWN_HOSTS_FILE` should point at a specific absolute
  known-hosts file for this run
- whether `-V` shows repeated `remote_capability_bootstrap_live`,
  `remote_capability_bootstrap_cache`, or `remote_cli_tool_direct_probes`
  counters that explain which startup probe path was active
- the stderr failure report and `last_command`

## Snapshot Discovery Failures

Example:

```text
Failed to retrieve snapshots from the source
```

What it usually means:

- remote `zfs list` failed
- remote `parallel` or `zstd` was missing or misresolved, and either the serial
  fallback also failed or the remaining snapshot-list command still could not
  execute
- source dataset naming or quoting was wrong on the remote side

What to inspect:

- stderr failure report
- source-side snapshot listing path
- whether `-V` shows the exact `Running remote probe [...]` or `Running remote
  command [...]` line for the failing discovery step
- whether `-V` shows source-discovery startup staying on cached capability
  data, waiting on an in-run cache fill, or falling back to a direct remote
  helper probe path
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

- `ZXFER_BACKUP_DIR` does not contain the expected exact keyed file
- the keyed file exists but does not contain one exact current-format
  `source,destination,properties` row for the requested pair
- secure file ownership / mode checks rejected the metadata
- the only available metadata is in an older unsupported layout

What to inspect:

- `ZXFER_BACKUP_DIR`
- whether `ZXFER_BACKUP_DIR` is set to an absolute path
- the source-dataset-relative tree under `ZXFER_BACKUP_DIR`
- the exact source/destination pair that was backed up with `-k`
- ownership and permissions of `.zxfer_backup_info.*`
- whether the backup file contains exactly one current-format row for the
  intended source/destination pair

## Failure Report Logging And Email Alerts

To persist every non-zero zxfer failure report, set `ZXFER_ERROR_LOG` to an
absolute path whose parent directory already exists, is owned by root or the
effective UID, and is not writable by other users unless the sticky bit is set:

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

zxfer serializes those appends through a sibling metadata-bearing lock
directory that records owner PID, process-start identity, hostname, purpose,
and creation time. Stale owners are reaped automatically after validation. If
a successful append cannot release that lock cleanly, the append helper now
fails closed and emits a warning; trap-time failure reporting still preserves
the original zxfer exit status while surfacing the warning on `stderr`.

The same owned-directory format now also backs shared ssh control-socket locks
and leases plus remote capability-cache locks under the validated temp root.
If you inspect temp roots while debugging startup or cleanup, expect `.lock`
paths and `leases/lease.*` entries to be directories with metadata files, not
bare pid files. Older plain ssh lease files and pid-only lock directories from
pre-metadata releases are unsupported; remove the stale entry or cache root if
zxfer reuses one during a rollout.

By default that block keeps `invocation` and `last_command` verbatim, so avoid
putting secret-bearing hook strings or wrapper arguments on the zxfer command
line unless you first enable redaction:

```sh
ZXFER_REDACT_FAILURE_REPORT_COMMANDS=1 \
ZXFER_ERROR_LOG=/var/log/zxfer/error.log \
./zxfer -v -R tank/src backup/dst
```

With redaction enabled, those two fields are replaced with `[redacted]` in both
`stderr` and `ZXFER_ERROR_LOG`.

Even without redaction, zxfer now escapes raw ASCII control bytes in structured
failure-report values before writing them, so terminal control sequences are
rendered inert in both `stderr` and `ZXFER_ERROR_LOG`.

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
requirements, rejects multiline `sendmail` header values in `ALERT_TO` and
`MAIL_FROM`, can iterate sequentially across a whitespace-separated
`SRC_DATASETS` list, includes stderr warnings outside the structured failure
block in the alert body, inherits
`ZXFER_REDACT_FAILURE_REPORT_COMMANDS=1` when you want the mailed report to
hide `invocation` and `last_command`, and can be validated with
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
