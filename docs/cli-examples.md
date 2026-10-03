# CLI Examples

This guide collects task-oriented `zxfer` command examples in one place. It is
meant to complement the man pages with copy-and-edit command lines that show
every current CLI flag in realistic combinations.

Review dataset names, hostnames, wrapper commands, and property lists before
running anything against a real pool.

## Placeholder Legend

- `SRC_ROOT`: source dataset root such as `tank/data`
- `SRC_FS`: one specific source filesystem such as `tank/data/app`
- `DEST_ROOT`: destination dataset root such as `backup/data`
- `DEST_FS`: one specific destination filesystem such as `backup/data/app`
- `ORIGIN_HOST`: remote source host such as `backup-src@example.com`
- `TARGET_HOST`: remote destination host such as `backup-dst@example.com`

## Basic Command Shapes

Recursive local replication:

```sh
./zxfer [options] -R SRC_ROOT DEST_ROOT
```

Non-recursive local replication:

```sh
./zxfer [options] -N SRC_FS DEST_FS
```

Pull from a remote source:

```sh
./zxfer [options] -O 'user@host [wrapper]' -R SRC_ROOT DEST_ROOT
```

Push to a remote destination:

```sh
./zxfer [options] -T 'user@host [wrapper]' -R SRC_ROOT DEST_ROOT
```

## Core Examples

### `-h` Print help

```sh
./zxfer -h
```

### `-v` Verbose mode

```sh
./zxfer -v -R tank/data backup/data
```

### `-V` Very verbose mode with profiling counters

```sh
./zxfer -V -R tank/data backup/data
```

This end-of-run profile now includes startup latency before the first live
send/receive pipeline, trap-cleanup timing, stage timings, ssh/zfs invocation
counts by role, runtime temp-file churn, command rendering, live destination
snapshot rechecks, and any remaining direct remote helper probes. Counter
keys and their order are stable across releases: counters for deleted
machinery (socket lock waits, capability cache waits and cache bootstraps,
cache-object writes and readbacks, other-side property reads) still print and
always read 0. While the run is
active, `-V` also prints
prefixed remote ssh commands, remote probe commands, and the ssh
control-master open and close commands (`Opening ssh control socket [...]`
before it opens, `Closing origin|target ssh control socket: ...` after it
closes), so a slow remote bootstrap shows the exact in-flight command.

The manual performance runner in `tests/run_perf_tests.sh` consumes these
profile lines when producing sample and summary artifacts. Prefer
`tests/run_vm_matrix.sh --profile smoke --test-layer perf` when you want that
measurement inside a disposable guest.

### `-n` Dry-run preview

```sh
./zxfer -n -v -R tank/data backup/data
```

Use this to preview rendered commands and preflight checks. Dry runs now stay
strictly no-exec: they skip ssh setup and remote helper validation (local
helpers are still resolved at startup), snapshot discovery, backup-restore
validation, unsupported-property detection, and `%%size%%` progress probes.
The `-o` checks still run, so a malformed or repeated `-o` property stops a dry
run too. Because strict dry-run no longer inspects live snapshot
state, it does not render the eventual send/receive or property-reconcile
commands. With `-k`, dry-run still previews secure backup-directory
preparation without touching the live backup store; there is no metadata write
to preview, because a dry run runs no property pass (`-v` prints "No property
data collected; skipping backup write.").

### `-R` Recursive replication

```sh
./zxfer -v -R tank/apps backup/apps
```

Replicates `tank/apps` and every descendant dataset beneath it.

### `-N` Non-recursive replication

```sh
./zxfer -v -N tank/apps/api backup/apps/api
```

Replicates only `tank/apps/api`.

### `-s` Take a fresh source snapshot before replication

```sh
./zxfer -v -s -R tank/data backup/data
```

### `-Y` Repeat until no sends or destroys are needed

```sh
./zxfer -v -Y -R tank/data backup/data
```

Each pass lists the source and destination afresh, so a destination that
another tool changed after one pass's discovery is seen by the next pass.
With `-s` or `-m`, only the first pass takes the snapshot (and, for `-m`,
stops the `-c` services and unmounts the source); later passes send what is
still missing, so a run takes one snapshot.

### `-j jobs` Run concurrent send/receive jobs

```sh
./zxfer -v -j 4 -R tank/projects backup/projects
```

`-j` still controls the send/receive job ceiling. When `jobs > 1`, zxfer also
uses the explicit per-dataset source-discovery path on the executing origin
host instead of the serial recursive listing. Local-origin and remote-origin
runs require a resolved `parallel` helper on the executing origin host. zxfer
intentionally validates only that the helper exists through the secure-PATH
model, then assumes the operator or package supplied an implementation
compatible with the GNU Parallel-style options used by the rendered pipeline.
If the helper is missing, zxfer fails closed during setup; if it is
incompatible, the source-discovery pipeline fails instead of silently falling
back to serial discovery. The
source-discovery helper is still tracked for cleanup by PID, while long-lived
send/receive workers record per-job status files. Abort cleanup uses verified
process groups where available, with a descendant-tracking wrapper as the
fallback. The send/receive scheduler treats
ancestor/descendant destination receives on the same target as mutually
exclusive, but it skips blocked descendants and starts later independent
datasets while job slots remain.

### `-x pattern` Exclude datasets from a recursive run

```sh
./zxfer -v -x '^tank/projects/(tmp|build-cache)$' -R tank/projects backup/projects
```

The pattern matches anywhere in a dataset name, so an unanchored `tmp` also
excludes `tank/projects/build-tmp`; anchor it, as above, to match exact names.

## Snapshot Cleanup And Safety

### `-d` Delete destination-only snapshots

```sh
./zxfer -v -d -R tank/data backup/data
```

Recursive cleanup applies to datasets present on the source. Datasets found
only on the destination and their snapshots are preserved; zxfer prints a
notice for skipped cleanup work and continues synchronizing the source tree.

### `-g days` Protect older destination snapshots from deletion

```sh
./zxfer -v -d -g 375 -R tank/data backup/data
```

This is usually paired with retention schemes where yearly snapshots should
survive even after newer monthly or daily snapshots are removed at the source.
With `-d`, zxfer checks every planned destination delete against `-g` before
it sends, receives, or destroys anything on the destination, and exits 2 if
one is protected; without `-d`, `-g` deletes nothing, but zxfer still plans
every dataset first and stops on a diverged destination before it sends,
receives, or destroys anything.

### `-F` Force rollback on the receive side

```sh
./zxfer -v -F -R tank/data backup/data
```

Use this when the destination may have diverged and should be rolled back to
the most recent snapshot that matches the stream.

When destination snapshots share source snapshot names but carry different
GUIDs (diverged data under identical names), zxfer always warns on stderr and
converges destructively (destroy the diverged destination snapshots, roll
back, resend) only when BOTH `-d` and `-F` are active; without both flags the
run fails closed for the diverged dataset. See `docs/troubleshooting.md` for
diagnosis commands.

## Property Handling

### `-P` Transfer source properties

```sh
./zxfer -v -P -R tank/data backup/data
```

### `-o property=value,...` Override destination properties

```sh
./zxfer -v -o 'compression=lz4,atime=off' -R tank/data backup/data
```

In recursive runs, zxfer sets the override on the replicated root and leaves
descendants inherited when the property can inherit and the parent already
provides the requested value. Non-inheritable overrides, such as quotas and
reservations, remain local on descendants.

Quote the full `-o` argument when one value needs a literal comma, and escape
that comma as `\,`:

```sh
./zxfer -v -o 'user:note=value\,with\,commas' -N tank/data/app backup/data/app
```

Name each property once. A property named twice, as in
`-o compression=lz4,compression=gzip`, or an item without `NAME=` is a usage
error (exit 2) found before any `zfs` command runs, so it also stops a `-n`
run. Every `-o` property must also exist on the source root; one that does not
stops the run, with exit 2, when the root's properties are read: before any
send or destination property change, but after discovery has listed the
destination and, with `-d`, after the root's destination-only snapshots are
destroyed.

### `-I properties,to,ignore` Skip selected properties

```sh
./zxfer -v -P -I 'quota,reservation' -R tank/data backup/data
```

### `-U` Skip properties unsupported by the destination

```sh
./zxfer -v -P -U -T backup@example.com -R tank/data backup/data
```

Useful when the current destination platform does not support every property
reported by the source within the OpenZFS 2+ support floor.

### `-k` Back up source properties before overriding them

```sh
ZXFER_BACKUP_DIR=/var/db/zxfer \
./zxfer -v -k -R tank/data backup/data
```

`-k` also enables property transfer so the destination still receives the live
source property set after the backup metadata is captured. `ZXFER_BACKUP_DIR`
must be an absolute path. Current-format backup files write the
`#format_version:2`, `#source_root`, and `#destination_root` header markers
before v2 source-root-relative property rows.

### `-e` Restore properties from a prior `-k` backup

```sh
ZXFER_BACKUP_DIR=/var/db/zxfer \
./zxfer -v -e -R tank/data backup/data
```

This looks up the current chunked lossless-keyed backup metadata path beneath
the source-dataset-relative tree under `ZXFER_BACKUP_DIR`, falls back read-only
to the retired checksum-keyed v2 filename when the current path is absent,
validates `#format_version:2`, then restores the matching source-root-relative
row. `-e` also flows through the property-transfer path during the restore.
The metadata is keyed by the source and destination of the `-k` run, so `-e`
must name the same pair, as above; any other pair, including the reverse
direction from the backup copy back to the original, stops with
`Cannot find backup property file`. `-e` reads the metadata on the source
side (the `-O` host with `-O`), while `-k` writes it on the destination side
(the `-T` host with `-T`).
Older mountpoint-local `.zxfer_backup_info.*` files and other legacy metadata
layouts are intentionally unsupported.
`ZXFER_BACKUP_DIR` must be a single-line absolute path without tabs or
carriage returns.

## Remote Replication And Stream Options

### `-O host` Pull from a remote origin host

```sh
./zxfer -v -O backup-src@example.com -R tank/data backup/data
```

Solaris or illumos wrapper-style host specs are supported:

```sh
./zxfer -v -O 'user1@solaris.example.com pfexec' -R tank/data backup/data
```

The `-O` and `-T` values are treated as literal whitespace-delimited tokens.
Outer shell quoting is fine, but embedded quote characters or backslash
escapes inside the value are rejected.

### `-T host` Push to a remote target host

```sh
./zxfer -v -T backup-dst@example.com -R tank/data backup/data
```

Destination discovery runs plain `zfs list` commands on the target, over the
target's ssh control master when the local ssh supports one, so the target
needs only `zfs` for it. If ssh itself fails during the destination snapshot
listing, zxfer stops with ssh's exit status 255.

### `-z` Compress the ssh stream with the default `zstd -3`

```sh
./zxfer -v -z -T backup-dst@example.com -R tank/data backup/data
```

`-z` requires either `-O` or `-T`. On remote-origin runs, the source snapshot
listings that run as one remote pipeline (the fast no-op proof's recursive
listing and the `-j` per-dataset listing) are compressed with the same
validated compression/decompression commands.

### `-Z command` Use a custom `zstd` compressor command

```sh
./zxfer -v -Z 'zstd -T0 -3' -T backup-dst@example.com -R tank/data backup/data
```

This still enables `-z`, but replaces the default `zstd` compressor with the
supplied command. The receive side still uses the matching decompression path.
Like `-O` and `-T`, the `-Z` value must be expressible as literal
whitespace-delimited tokens; embedded quote characters or backslash escapes are
rejected instead of being re-tokenized.

### `-D command` Pipe the send stream through a progress command

```sh
./zxfer -v -D 'pv -brt -s %%size%% -N %%title%%' -R tank/data backup/data
```

zxfer tees the send stream into a private FIFO that the progress command reads,
so `-D` also works with `-j`. The progress command must read the copied stream
from stdin until EOF. Its stdout is discarded so progress helpers such as `pv` cannot duplicate or
corrupt the receive stream; progress text should go to stderr. `%%size%%`
expands to an estimated stream size and `%%title%%` expands to the source
`dataset@snapshot` label.

### `-w` Use raw `zfs send`

```sh
./zxfer -v -w -T vault@example.com -R tank/secure backup/secure
```

Raw sends are commonly used for encrypted datasets when the original raw stream
must be preserved.

## Migration And Service Handling

### `-m` Migrate the source mountpoint to the destination

```sh
./zxfer -v -m -N tank/apps/api backup/cutover/api
```

`-m` implies `-s` and `-P`, and it is local-only.

### `-c 'service list'` Temporarily disable SMF services during migration

```sh
./zxfer -v -m -c 'svc:/network/nfs/server:default svc:/application/web:default' \
	-N tank/apps/api backup/cutover/api
```

`-c` requires `-m` and SMF: without `svcadm` it is a usage error, so it works
only on illumos or Solaris, where services should be disabled before
unmounting the source.

## Notifications

### `-b` Beep on failure after long-running work

```sh
./zxfer -b -v -R tank/data backup/data
```

### `-B` Beep on success or failure

```sh
./zxfer -B -v -R tank/data backup/data
```

Use `-B` only on the last `zxfer` invocation in a script; use `-b` on earlier
steps if you want only failure alerts. Beeps need the FreeBSD speaker(4)
device; on other hosts zxfer skips them.

## Composite Recipes

### Conservative recursive backup with delete, rollback, properties, and GFS protection

```sh
ZXFER_BACKUP_DIR=/var/db/zxfer \
./zxfer -d -F -g 375 -k -P -v -R tank/storage backup01/pools
```

### Remote pull with concurrency, deletion, and convergence loops

```sh
./zxfer -v -d -F -j 8 -Y -O backup-src@example.com -R zroot tank/backups/zroot
```

### Remote push with custom compression and a progress display

```sh
./zxfer -v -d -Z 'zstd -T0 -3' \
	-D 'pv -brt -s %%size%% -N %%title%%' \
	-T backup-dst@example.com -R tank/archive backup/archive
```

### Recursive property transfer while skipping incompatible destination properties

```sh
./zxfer -v -P -U -I 'quota,reservation' -T backup-omnios.example.com \
	-R tank/home backup/home
```

### Property backup before an override, then restore the original properties

```sh
ZXFER_BACKUP_DIR=/var/db/zxfer \
./zxfer -v -k -o 'compression=gzip,atime=off' -R tank/app backup/app

ZXFER_BACKUP_DIR=/var/db/zxfer \
./zxfer -v -e -R tank/app backup/app
```

The second run sets the source properties that the first one recorded on
`backup/app/app` and its descendants, undoing the overrides.

### Local cutover migration with service disable, fresh snapshot, and property sync

```sh
./zxfer -v -m -c 'svc:/network/nfs/server:default' \
	-N tank/prod/api backup/cutover/api
```

### Raw encrypted push over ssh

```sh
./zxfer -v -w -z -T vault@example.com -R tank/secure backup/secure
```

## Important Option Rules

- Use exactly one of `-R` or `-N`.
- `-m` and `-c` cannot be combined with `-O` or `-T`.
- `-c` requires `-m`.
- `-z` and `-Z` require `-O` or `-T`.
- `-Z` also enables `-z`.
- `-k` and `-e` cannot be combined.
- `-k`, `-e`, and `-m` all imply `-P`.
- Name each `-o` property once; a repeated property or an item without
  `NAME=` is a usage error (exit 2).
- `-b` and `-B` cannot be combined.
- Avoid using `-O` and `-T` together unless you intentionally want the local
  host to relay traffic between two remote systems.
- An option that takes an argument must end its cluster:
  `-vFdR tank/src backup/dst` works, but in `-Rv tank/src backup/dst`
  getopts takes `v` as the `-R` source and `tank/src` becomes the
  destination. Giving such options separately, as in
  `-vFd -R tank/src backup/dst`, avoids the mistake.

## Related References

- [../man/zxfer.8](../man/zxfer.8): full option semantics
- [../man/zxfer.1m](../man/zxfer.1m): Solaris/illumos man page variant
- [../examples/README.md](../examples/README.md): runnable shell templates for
  common workflows
