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

This end-of-run profile now includes stage timings plus contention and reuse
counters for ssh control-socket waits, remote-capability cache waits,
capability-bootstrap sources (`live`, `cache`, `memory`), and any remaining
direct remote helper probes. While the run is active, `-V` also prints
prefixed remote ssh commands, remote probe commands, and ssh control-socket
check/open commands so a slow remote bootstrap shows the exact in-flight
command.

### `-n` Dry-run preview

```sh
./zxfer -n -v -R tank/data backup/data
```

Use this to preview rendered commands and preflight checks. Dry runs now stay
strictly no-exec: they skip live helper resolution, snapshot discovery,
backup-restore validation, unsupported-property detection, and `%%size%%`
progress probes. Because strict dry-run no longer inspects live snapshot
state, it does not render the eventual send/receive or property-reconcile
commands. With `-k`, dry-run still previews secure backup-directory
preparation plus the staged metadata-write commands, including chained
provenance alias writes, without touching the live backup store.

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

### `-j jobs` Run concurrent send/receive jobs

```sh
./zxfer -v -j 4 -R tank/projects backup/projects
```

`-j` still controls the send/receive job ceiling, and on origin-side runs it
also enables adaptive source snapshot discovery that can use GNU `parallel`
when the validated helper is available. Local origin runs fall back to the
serial discovery path when the helper is missing or is not GNU `parallel`.
Remote origin runs still fall back for the explicit missing-helper case, but
other remote helper probe or execution failures abort the run instead of being
silently treated as a serial fallback.

### `-x pattern` Exclude datasets from a recursive run

```sh
./zxfer -v -x '^tank/projects/(tmp|build-cache)$' -R tank/projects backup/projects
```

Use anchored expressions when you want exact matches instead of prefix matches.

## Snapshot Cleanup And Safety

### `-d` Delete destination-only snapshots

```sh
./zxfer -v -d -R tank/data backup/data
```

### `-g days` Protect older destination snapshots from deletion

```sh
./zxfer -v -d -g 375 -R tank/data backup/data
```

This is usually paired with retention schemes where yearly snapshots should
survive even after newer monthly or daily snapshots are removed at the source.

### `-F` Force rollback on the receive side

```sh
./zxfer -v -F -R tank/data backup/data
```

Use this when the destination may have diverged and should be rolled back to
the most recent snapshot that matches the stream.

## Property Handling

### `-P` Transfer source properties

```sh
./zxfer -v -P -R tank/data backup/data
```

### `-o property=value,...` Override destination properties

```sh
./zxfer -v -o 'compression=lz4,atime=off' -R tank/data backup/data
```

Quote the full `-o` argument when one value needs a literal comma, and escape
that comma as `\,`:

```sh
./zxfer -v -o 'user:note=value\,with\,commas' -N tank/data/app backup/data/app
```

### `-I properties,to,ignore` Skip selected properties

```sh
./zxfer -v -P -I 'quota,reservation' -R tank/data backup/data
```

### `-U` Skip properties unsupported by the destination

```sh
./zxfer -v -P -U -T old-backup@example.com -R tank/data backup/data
```

Useful when replicating to an older OpenZFS host that does not understand every
property supported by the source.

### `-k` Back up source properties before overriding them

```sh
ZXFER_BACKUP_DIR=/var/db/zxfer \
./zxfer -v -k -R tank/data backup/data
```

`-k` also enables property transfer so the destination still receives the live
source property set after the backup metadata is captured. `ZXFER_BACKUP_DIR`
must be an absolute path. Current-format backup files write the
`#format_version`, `#source_root`, and `#destination_root` header markers
before the exact source/destination property rows.

### `-e` Restore properties from a prior `-k` backup

```sh
ZXFER_BACKUP_DIR=/var/db/zxfer \
./zxfer -v -e -R tank/data backup/restore-data
```

This looks up the exact keyed backup metadata file beneath the current
source-dataset-relative tree under `ZXFER_BACKUP_DIR`.
`-e` also flows through the property-transfer path during the restore.
Older mountpoint-local `.zxfer_backup_info.*` files and other legacy metadata
layouts are intentionally unsupported. `ZXFER_BACKUP_DIR` must be an absolute
path.

## Remote Replication And Stream Options

### `-O host` Pull from a remote origin host

```sh
./zxfer -v -O backup-src@example.com -R tank/data backup/data
```

Solaris or illumos wrapper-style host specs are supported:

```sh
./zxfer -v -O 'user1@solaris.example.com pfexec' -R tank/data backup/data
```

### `-T host` Push to a remote target host

```sh
./zxfer -v -T backup-dst@example.com -R tank/data backup/data
```

### `-z` Compress the ssh stream with the default `zstd -3`

```sh
./zxfer -v -z -T backup-dst@example.com -R tank/data backup/data
```

`-z` requires either `-O` or `-T`. On remote-origin runs that also use
adaptive `-j` source discovery, the same validated compression/decompression
pipeline is reused for the source snapshot-list metadata stream.

### `-Z command` Use a custom `zstd` compressor command

```sh
./zxfer -v -Z 'zstd -T0 -3' -T backup-dst@example.com -R tank/data backup/data
```

This still enables `-z`, but replaces the default `zstd` compressor with the
supplied command. The receive side still uses the matching decompression path.

### `-D command` Pipe the send stream through a progress command

```sh
./zxfer -v -D 'pv -brt -s %%size%% -N %%title%%' -R tank/data backup/data
```

The progress command must read from stdin and write the stream back to stdout.
`%%size%%` expands to an estimated stream size and `%%title%%` expands to the
source `dataset@snapshot` label.

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

`-c` requires `-m` and is primarily for illumos or Solaris migrations where
services should be disabled before unmounting the source.

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
steps if you want only failure alerts.

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
./zxfer -v -P -U -I 'quota,reservation' -T old-backup.example.com \
	-R tank/home backup/home
```

### Property backup before an override, then restore to a new destination

```sh
ZXFER_BACKUP_DIR=/var/db/zxfer \
./zxfer -v -k -o 'mountpoint=/srv/restore,atime=off' -R tank/app backup/app

ZXFER_BACKUP_DIR=/var/db/zxfer \
./zxfer -v -e -R tank/app backup/app-restored
```

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
- `-b` and `-B` cannot be combined.
- Avoid using `-O` and `-T` together unless you intentionally want the local
  host to relay traffic between two remote systems.
- Options that take arguments should not be glued to later flags. Use
  `-vFd -R tank/src backup/dst`, not `-vFdR tank/src backup/dst`.

## Related References

- [../man/zxfer.8](../man/zxfer.8): full option semantics
- [../man/zxfer.1m](../man/zxfer.1m): Solaris/illumos man page variant
- [../examples/README.md](../examples/README.md): runnable shell templates for
  common workflows
