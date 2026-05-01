# What's New Since v1.1.7

This page is the upgrade guide from the legacy
`v1.1.7` release from February 19, 2019 to the current maintained fork.

It does not list every internal refactor, CI update, or test harness change.
Instead, it focuses on the changes that affect real usage, compatibility, and
upgrade planning.

If you only read two sections, read:

- [Breaking Changes And Removed Behavior](#breaking-changes-and-removed-behavior)
- [Upgrade Checklist](#upgrade-checklist)

## At A Glance

Compared with `v1.1.7`, current zxfer is:

- ZFS-native only. The old rsync mode is gone.
- Much stricter about safety and security. Helper lookup, ssh handling,
  property backup metadata, and failure reporting have all been hardened.
- Better at large recursive replications. Concurrency, snapshot discovery,
  property reconciliation, and delete planning have all been reworked.
- Better documented and better tested across current OpenZFS 2.0+ workflows on
  FreeBSD, Linux, OmniOS/illumos, and OpenZFS on macOS.

## Breaking Changes And Removed Behavior

These are the changes most likely to require script or workflow updates.

### 1. rsync mode is gone

`v1.1.7` supported both ZFS send/receive replication and rsync-based copying.
The current fork supports ZFS-native replication only.

Removed behavior:

- `-S` rsync mode
- rsync-only options such as `-E`, `-f`, `-i`, `-l`, `-L`, `-p`, and `-u`

What to do:

- If you relied on rsync mode, stay on an older release for that workflow or
  switch that part of your process to another tool.
- For ZFS snapshot replication, use current zxfer directly.
- Within this repository, `upstream-compat-final` is the closest
  fork-specific reference branch from before rsync removal, while
  `upstream-archive` mirrors the latest imported upstream
  [allanjude/zxfer](https://github.com/allanjude/zxfer) history for easy
  comparison.

### 2. Property restore now requires current backup metadata

Legacy property-backup compatibility has been removed.

Current behavior:

- `-k` writes hardened backup metadata under `ZXFER_BACKUP_DIR`
- `-e` restores only from the current keyed, versioned metadata layout
- current metadata writes use the versioned root markers
  `#format_version:2`, `#source_root`, and `#destination_root`
- older or unversioned restore metadata layouts are no longer supported

This is one of the biggest upgrade breaks for long-lived installs.

What to do:

- If you need future restores with the current fork, create fresh `-k`
  backups with the current zxfer before depending on `-e`.
- Do not assume old `.zxfer_backup_info.*` files from `v1.1.7` will restore
  cleanly on the current fork.

### 3. Backup metadata no longer lives in dataset mountpoints

Older zxfer workflows could rely on metadata files located near the replicated
datasets. Current zxfer stores backup metadata in a hardened directory tree
under `ZXFER_BACKUP_DIR`, which defaults to `/var/db/zxfer`.

Current behavior:

- backup metadata is stored outside dataset mountpoints
- metadata files are keyed by the source/destination pair
- restore reads validate the file format before consuming it

This is safer, but it is a workflow change if you were inspecting or managing
backup metadata in the old locations.

### 4. Helper lookup is now intentionally strict

`v1.1.7` used ambient `PATH` lookups such as `which zfs`, `which awk`, and
similar helper discovery. Current zxfer rebuilds `PATH` from a trusted
allowlist and resolves required tools to absolute paths.

Current behavior:

- local helper lookup uses a secure PATH model
- remote helper lookup resolves `zfs`, `cat`, `parallel` when `-j > 1`
  requires it, and compression helpers on the remote host
- zxfer does not assume the same absolute helper paths exist everywhere

What to do:

- If your environment depends on non-standard helper locations, set
  `ZXFER_SECURE_PATH` or `ZXFER_SECURE_PATH_APPEND`.
- Do not assume a custom shell profile or login shell PATH will be enough.

### 5. zxfer-managed ssh defaults are stricter

Current zxfer treats ssh more defensively than `v1.1.7`.

Current behavior:

- zxfer-managed ssh defaults to `BatchMode=yes`
- zxfer-managed ssh defaults to `StrictHostKeyChecking=yes`
- host-specific helper resolution and control-socket handling are stricter

What to do:

- If you need a pinned known-hosts file, set
  `ZXFER_SSH_USER_KNOWN_HOSTS_FILE`.
- If you intentionally want ambient local ssh behavior, set
  `ZXFER_SSH_USE_AMBIENT_CONFIG=1`.

### 6. More failures now abort the run instead of being masked

This is a deliberate safety change.

Current zxfer now fails closed for many conditions that older versions could
mis-handle, under-report, or silently treat as empty success.

Common examples:

- unreadable or invalid backup metadata
- remote helper probe failures
- destination existence or snapshot recheck failures
- snapshot planning failures
- cache or staged-tempfile readback failures
- property probe failures that make the result unsafe to trust

If an old script relied on zxfer "doing its best" through those states, expect
current zxfer to stop earlier and emit a structured error instead.

### 7. Snapshot identity is stricter

`v1.1.7` was much more trusting about snapshot names. Current zxfer matches
snapshots by identity more carefully and uses GUID-aware logic in the modern
replication path.

Practical effect:

- same-named but unrelated snapshots are less likely to be treated as a valid
  incremental base
- delete and rollback planning is more conservative and more accurate

### 8. `-d` is safer around rollback than older workflows expected

Current zxfer no longer uses the more aggressive destructive rollback path
unless the caller explicitly opts into forced receive behavior with `-F`.

Practical effect:

- if you had older cleanup or resend workflows that assumed `-d` would roll
  back the destination by itself, review them carefully
- current zxfer expects destructive receive behavior to be explicit

## Important New Capabilities Since v1.1.7

These are the biggest user-visible additions since the 2019 release.

### New CLI options and behaviors

- `-j jobs`: concurrent send/receive execution with explicit per-dataset
  source discovery when `jobs > 1`; local-origin and remote-origin runs
  require a resolved `parallel` helper on the executing origin host. zxfer
  checks helper existence through the secure-PATH model and intentionally
  leaves GNU Parallel-style compatibility to operators and packages. Source
  discovery uses tracked background PID cleanup,
  and long-lived send/receive workers use supervisor-backed teardown instead of
  bare wrapper-shell PID cleanup
- `-V`: very verbose debug output plus profiling counters
- `-w`: raw `zfs send`
- `-x pattern`: exclude matching datasets from recursive replication
- `-Y`: repeat replication until no sends or destroys are performed, up to the
  built-in iteration cap
- `-z`: `zstd` compression for ssh replication
- `-Z "command"`: custom `zstd` compressor command such as `zstd -T0 -3`

### Better remote-host handling

- wrapper-style remote host specs such as `user@host pfexec` or
  `user@host doas` are supported and handled more safely
- remote helper resolution is per-host instead of assuming the local helper
  path exists remotely
- ssh control sockets can be reused within one run and across sibling zxfer
  processes through a validated per-user cache
- shared ssh control sockets and remote capability cache fills are coordinated
  through metadata-bearing lock/lease directories instead of ad hoc pid files,
  with stale-owner validation and checked release semantics

### Better property handling

- unsupported-property handling is much more deliberate
- property replication and reconciliation are more robust for mixed-platform
  replication
- `-k` and `-e` are safer, stricter, and more explicit about metadata format

### Better reporting

- non-zero exits now emit a structured stderr failure report
- failures can also be mirrored to `ZXFER_ERROR_LOG`
- command-bearing fields inside failure reports are redacted by default, with
  an explicit unsafe local-debug override through
  `ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1`
- `ZXFER_ERROR_LOG` appends now use the same metadata-bearing lock format as
  the ssh and remote-capability coordination layer, including checked release
  behavior that preserves the original zxfer exit status during trap cleanup

## Behavior Changes That May Surprise Old Automation

These are not removals, but they are common upgrade surprises.

### Error output is richer and more structured

If you parse stderr, expect different output than `v1.1.7`. Current zxfer
surfaces more context on non-zero exits.

### Lock and lease state under TMPDIR is no longer plain pid files

If you had automation that inspected or deleted zxfer runtime lock state
directly, re-test it. Current native shared-state entries are metadata-bearing
directories:

- ssh control-socket `.lock` paths are directories with owner metadata
- ssh `leases/lease.*` entries are directories instead of plain files
- remote capability `<cache>.lock` paths are metadata-bearing directories

Older plain ssh lease files and pid-only `.lock` directories are no longer
supported. Clear stale old cache roots before the first current-release run
instead of relying on mixed-format upgrade compatibility.

### Large recursive runs behave differently

Current zxfer has significantly different logic for:

- recursive snapshot discovery
- concurrent send/receive job scheduling
- destination delete planning
- property caching and reconciliation
- remote capability detection

The goal is higher safety and better scaling, not behavioral compatibility
with the 2019 internal implementation.

### Empty-destination and pre-created-destination flows are better supported

A large amount of post-`v1.1.7` work went into first-run replication, seeded
destinations, and property transfer to pre-created targets. If you carried
local wrapper logic to compensate for older first-run edge cases, re-test
whether you still need it.

## Release-Train Overview

### 2024 release train

- performance-oriented refactors for snapshot discovery and recursive
  replication
- modular source layout under `src/`
- new options such as `-w`, `-Y`, and `-z`
- initial concurrency and very-verbose improvements

### 2025 release train

- major security hardening for helper lookup, ssh construction, backup
  metadata, and property application
- initial shunit2 and integration coverage, plus the first GitHub Actions
  test and lint workflows
- rsync mode removal and the shift to a ZFS-native-only tool

### 2026 current branch

- broad fail-closed reliability work
- stricter cache, temp-file, and metadata validation
- better remote capability probing and diagnostics
- metadata-bearing owned lock/lease coordination for shared ssh control
  sockets, remote capability caches, and `ZXFER_ERROR_LOG` appends
- repository reorganization into `docs/`, `examples/`, `man/`, and
  `packaging/`, plus a much broader documentation set
- stronger VM-backed validation and expanded CI coverage

## Upgrade Checklist

Use this checklist when moving a legacy install or automation from `v1.1.7`.

1. Remove any use of `-S` and any rsync-only flags.
2. Re-read the current man page and check every option your scripts use.
3. Recreate property backups with current `-k` before relying on `-e`.
4. Audit helper-path assumptions and configure `ZXFER_SECURE_PATH` or
   `ZXFER_SECURE_PATH_APPEND` if needed.
5. Audit ssh trust assumptions and set
   `ZXFER_SSH_USER_KNOWN_HOSTS_FILE` or
   `ZXFER_SSH_USE_AMBIENT_CONFIG=1` if your workflow requires it.
6. Re-test any `-d` and `-F` workflows on throwaway datasets before using
   them on production data.
7. Re-test any automation that parses stderr or depends on older error text.
8. Re-test remote wrapper-host workflows such as `pfexec` and `doas`.

## Where To Read Next

- [../README.md](../README.md): current project overview and quick start
- [../man/zxfer.8](../man/zxfer.8): primary command reference
- [../man/zxfer.1m](../man/zxfer.1m): Solaris/illumos command reference
- [platforms.md](./platforms.md): platform caveats and compatibility notes
- [testing.md](./testing.md): validation and integration guidance
- [upstream-history.md](./upstream-history.md): historical context and removed
  legacy behavior
- [../CHANGELOG.txt](../CHANGELOG.txt): complete chronological change log
