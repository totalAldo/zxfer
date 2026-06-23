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
- remote `parallel` or `zstd` was missing, incompatible, or misresolved,
  so the rendered snapshot-list command could not execute
- source dataset naming or quoting was wrong on the remote side
- the tracked background helper could not launch, complete, or publish readable
  source snapshot output and stderr

What to inspect:

- stderr failure report
- source-side snapshot listing path
- whether `-V` shows the exact `Running remote probe [...]` or `Running remote
  command [...]` line for the failing discovery step
- whether `-V` shows source-discovery startup staying on cached capability
  data, waiting on an in-run cache fill, or falling back to a direct remote
  helper probe path
- any shell quoting problems on the remote host
- whether the source snapshot output file was empty or unreadable, and whether
  the paired staged stderr file contains the original `zfs list`, `parallel`,
  remote helper, or compression error
- temp-root permissions if the failure points at launching the background
  helper or registering the cleanup PID rather than the `zfs list` itself

## Destination Divergence Warnings And Failures

Example:

```text
WARNING: destination dataset [backup/pool/data] has 2 snapshot(s) whose names
match source dataset [pool/data] but whose guids differ ...
```

or, without both `-d` and `-F`:

```text
Destination dataset [backup/pool/data] has diverged from source dataset
[pool/data]: ... Re-run with BOTH -d and -F to converge destructively ...
```

What it usually means:

- the destination dataset received writes outside this replication chain
  (another tool, another zxfer invocation against a different source, or a
  manual `zfs receive`/`zfs rollback`) and then re-created snapshots whose
  NAMES match the source while their data (GUIDs) differ
- a snapshot-management tool on the destination is regenerating snapshots
  under the same naming scheme as the source
- the source was rolled back or rebuilt after the destination had already
  received the original snapshots

Step 0 — confirm the divergence yourself before authorizing destruction.
Compare both sides' GUIDs for the named dataset (run the second command on
the destination host when replicating remotely):

```sh
zfs list -H -d 1 -o name,guid -s creation -t snapshot pool/data
zfs list -H -d 1 -o name,guid -s creation -t snapshot backup/pool/data
```

Same snapshot name with a different GUID on the two sides is exactly what the
warning is reporting. The newest name with a MATCHING GUID is the last common
snapshot zxfer will keep and roll back to.

What to do:

- if the destination copies of the diverged snapshots are disposable, re-run
  with BOTH `-d` and `-F`: zxfer destroys the diverged destination snapshots,
  rolls back to the last GUID-matching common snapshot, and resends the
  source range over them (this is destructive on the destination side)
- if the destination data must be preserved, reconcile manually first: rename
  the diverged destination snapshots out of the way, `zfs clone` what you
  need to keep, or replicate the destination dataset elsewhere, then re-run
- if a run with `-d -F` fails with `re-diverged after convergence`, an
  external writer is re-creating diverged snapshots while zxfer converges the
  dataset; stop that writer (look for snapshot tools or other replication
  jobs on the destination host) before re-running
- with `-V`, planning prints `Last common snapshot: ...; diverged destination
  snapshots: N.` per planned dataset and the profile summary reports
  `diverged_snapshot_warnings` so unattended runs can be audited

## Background Completion Failures

Examples:

```text
Failed to read zfs send/receive completion metadata for [tank/src@snap2 -> backup/dst].
Failed to publish zfs send/receive background completion for [tank/src@snap2 -> backup/dst] (PID 12345, exit 0).
Failed to report zfs send/receive background completion for [tank/src@snap2 -> backup/dst] (PID 12345, exit 1).
```

What it usually means:

- the background job shell finished, but its per-job status file could not be
  written or reloaded (a missing or malformed status file at wait time means
  the job shell died before recording its status)
- the completion queue notification could not be published back to the parent
  process
- the runtime temp root became unreadable, unwritable, or was removed mid-run

What to inspect:

- stderr failure report and `last_command`
- temp-root ownership and permissions
- the first dataset-aware `zfs send/receive job failed for [...]` line earlier in
  stderr, because later `zstd: unexpected end of file` or `cannot receive:
  failed to read from stream` messages are often collateral after zxfer aborts
  sibling background jobs on the first real failure
- whether the failure is isolated to queue publication (`publish`) or
  status-file persistence/readback (`read` / `report`)

## Performance Harness Results

`tests/run_perf_tests.sh` is informative first. Baseline regressions write
warnings and `compare.tsv`, but they do not fail the run. A non-zero harness
exit means setup, zxfer execution, cleanup, or replication correctness failed.

What to inspect:

- `summary.md` for the readable per-case average
- `summary.tsv` for the machine-readable baseline to preserve with a PR or
  local benchmark note
- `samples.tsv` for each warmup and measured sample
- `raw/<case>/*.stderr` for `zxfer profile: key=value` lines and structured
  failure reports
- `raw/<case>/*.mock_ssh.log` when a remote-mock case changes round-trip
  counts

If `startup_latency_ms` is `0`, zxfer did not dispatch a live send/receive
pipeline for that sample. If `cleanup_ms` increases unexpectedly, inspect the
same stderr log for trap-time cleanup warnings before treating the sample as a
pure throughput regression.

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

- `ZXFER_BACKUP_DIR` does not contain the expected current chunked
  lossless-keyed metadata path or the retired checksum-keyed v2 fallback file
- the keyed file exists but does not contain a current-format v2 relative row
  for the requested source under `#source_root` / `#destination_root`
- secure file ownership / mode checks rejected the metadata
- the only available metadata is in an older unsupported layout

What to inspect:

- `ZXFER_BACKUP_DIR`
- whether `ZXFER_BACKUP_DIR` is set to an absolute path
- the source-dataset-relative tree under `ZXFER_BACKUP_DIR`
- the exact source/destination roots that were backed up with `-k`
- ownership and permissions of `.zxfer_backup_info.*`
- whether the backup file contains exactly one current-format relative row for
  the intended dataset

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

zxfer serializes those appends through a metadata-bearing lock directory that
records owner PID, process-start identity, hostname, purpose, and creation
time. Writable log parents use a sibling lock; existing logs under trusted but
non-writable parents use an exact log-path fallback lock under a validated temp
root. Stale owners are reaped automatically after validation. If a successful
append cannot release that lock cleanly, the append helper now fails closed and
emits a warning; trap-time failure reporting still preserves the original zxfer
exit status while surfacing the warning on `stderr`.

Current ssh control sockets and remote capability state are per-run instead of
shared. If you inspect a live run's temp root while debugging startup or
cleanup, expect at most short `ssh-<role>.sock` control sockets owned by that
invocation; remote helper capabilities are held in memory and have no cache or
lock files. Older shared ssh lease directories, pid-only socket locks, and
remote capability-cache lock roots from pre-per-run branch builds are stale
artifacts rather than current zxfer state.

By default that block redacts `invocation` and `last_command` as `[redacted]`,
so routine failure logs do not capture raw command lines. If you explicitly
need verbatim command text during local debugging, you can opt into the unsafe
mode:

```sh
ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1 \
ZXFER_ERROR_LOG=/var/log/zxfer/error.log \
./zxfer -v -R tank/src backup/dst
```

With `ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1`, those two fields are emitted
verbatim in both `stderr` and `ZXFER_ERROR_LOG`, so use that mode only for
deliberate local debugging on trusted log sinks.

Even in unsafe verbatim mode, zxfer escapes raw ASCII control bytes in
structured failure-report values before writing them, so terminal control
sequences are rendered inert in both `stderr` and `ZXFER_ERROR_LOG`.

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
block in the alert body, inherits the default safe redaction, and honors
`ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1` only when you deliberately want the
mailed report to include verbatim `invocation` and `last_command`. The wrapper
can be validated with
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
