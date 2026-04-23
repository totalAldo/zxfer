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

### Medium: `--ensure-writable` can rewrite user property values that merely contain `readonly=on`

`src/zxfer_property_reconcile.sh` currently implements
`zxfer_force_readonly_off()` with a global text substitution over the entire
serialized property list. When `g_ensure_writable=1`, that rewrites any
property value containing the literal substring `readonly=on`, not just the
actual `readonly` property entry. As a result, user properties such as
`user:note=readonly=on=local` are silently changed to
`user:note=readonly=off=local` during property reconciliation and backup-based
restore flows.

### Medium: property names containing regex metacharacters can be filtered incorrectly

`src/zxfer_property_reconcile.sh` removes matched property names from working
filter lists with `grep -v ^"$l_property"$`. That treats the property name as
a regular expression instead of a literal token. Valid user property names
that contain regex metacharacters such as `.` can therefore remove or skip
unrelated properties that merely match the regex. For example, matching
`user:a.b` also removes `user:axb` from the remaining list, which can cause
must-create selection and readonly/ignore filtering to produce incomplete or
incorrect property sets.

### Medium: destination creates with `-p` can silently drop create-time properties

`src/zxfer_property_reconcile.sh` builds `zfs create -p ... -o property=value`
commands in `zxfer_run_zfs_create_with_properties()` whenever it needs to
create missing parents, and `zxfer_ensure_destination_exists()` forces that
path for every non-root child create plus root creates under a missing
destination prefix. But the OpenZFS `zfs-create(8)` man page documents that
with `-p`, any `-o` property arguments are ignored. In a direct helper repro,
`zxfer_run_zfs_create_with_properties yes volume 10G "compression=lz4,atime=off" backup/dst`
renders `create -p -V 10G -o compression=lz4 -o atime=off backup/dst`, so
zxfer can believe it created a child or nested root with required creation
properties even though the target ZFS implementation ignores them. That can
silently lose must-create settings such as filesystem or volume creation
properties whenever the receive path needs to create missing parent datasets.

### Medium: `-U` still replays unsupported creation properties into missing child creates

`src/zxfer_property_reconcile.sh` strips dataset-type-specific unsupported
properties from `l_override_pvs` in `zxfer_transfer_properties()`, but it never
applies the same filtering to `l_creation_pvs` before calling
`zxfer_ensure_destination_exists()`. The missing-child create path then only
sanitizes readonly and ignored properties before handing the raw creation list
to `zxfer_run_zfs_create_with_properties()`. In a direct helper repro with
`g_option_U_skip_unsupported_properties=1` and
`g_unsupported_properties=casesensitivity`, running
`zxfer_transfer_properties tank/src/child` for a missing destination child
still renders `CREATE with_parents=yes props=casesensitivity=sensitive,compression=lz4`.
That means `-U` can still drive `zfs create` with an unsupported creation-time
property such as `casesensitivity`, aborting child dataset creation even though
the run was explicitly told to skip unsupported destination properties.

### Medium: missing-dataset creates can drop must-create source properties when `-P` is off

`src/zxfer_property_reconcile.sh` explicitly backfills required creation-time
properties into the source property sets with
`zxfer_ensure_required_properties_present()`, but
`zxfer_derive_override_lists()` immediately exits when
`g_option_P_transfer_property=0` and never replays those source properties into
either `override_pvs` or `creation_pvs`. `zxfer_transfer_properties()` then
passes those truncated lists into `zxfer_ensure_destination_exists()`, which
uses them to create any missing root or child dataset. In a direct helper repro
with source properties `casesensitivity=sensitive=local,compression=off=local`,
`-P` disabled, `-o compression=lz4`, and a missing destination,
`zxfer_transfer_properties tank/src` passed `override=compression=lz4=override`
and `creation=` into the create path. That means zxfer can create a dataset
without required immutable creation properties such as `casesensitivity`,
`normalization`, or `utf8only`, even though it already probed and backfilled
those properties from the source.

### Medium: parent property mutations leave descendant destination-property caches stale

`src/zxfer_property_reconcile.sh` invalidates only the exact destination path
after successful `zfs set` and `zfs inherit` operations by calling
`zxfer_invalidate_destination_property_cache "$l_destination"`, and
`src/zxfer_property_cache.sh` removes only that one dataset cache entry. But
normalized destination-property lookups consult child cache files first and
return them without any live probe. In a direct helper repro, seeding a cached
`backup/dst/child` payload of `compression=gzip=inherited`, then running
`zxfer_run_zfs_set_property compression lz4 backup/dst`, still leaves the child
cache file in place and `zxfer_get_normalized_dataset_properties
backup/dst/child /sbin/zfs destination` returns the stale cached
`compression=gzip=inherited` payload without touching `zfs get`. That means a
parent property change can leave later child reconciliation reasoning from an
old inherited destination state, driving the wrong set/inherit diff for the
rest of the recursive run.

### Medium: destination snapshot normalization rewrites repeated destination-root path segments past the leading prefix

`src/zxfer_snapshot_discovery.sh` maps destination snapshot paths back into the
source namespace with `sed -e "s|$l_escaped_destination_dataset|$g_initial_source|g"`,
which replaces every occurrence of the destination dataset path on each line
 instead of only the leading dataset prefix. In a direct helper repro with
`g_initial_source=tank/src` and destination snapshot record
`backup/dst/backup/dst/child@snap1\t111`,
`zxfer_normalize_destination_snapshot_list backup/dst ...` produced
`tank/src/tank/src/child@snap1\t111` instead of the correct
`tank/src/backup/dst/child@snap1\t111`. That means recursive discovery can
corrupt mapped destination snapshot paths whenever a descendant subtree repeats
the destination-root path later in the dataset name, which can hide true common
snapshots or fabricate divergence during delete and transfer planning.

### Medium: live destination rechecks keep a stale common-snapshot anchor when no live match remains

`src/zxfer_replication.sh` refreshes `g_last_common_snap` and
`g_src_snapshot_transfer_list` from live destination snapshots before sending,
but `zxfer_reconcile_live_destination_snapshot_state()` returns early when the
live destination has snapshots and none of them match the current transfer
list. In that case it leaves the previously cached `g_last_common_snap` and
remaining transfer list untouched while still setting `g_dest_has_snapshots=1`.
Later copy planning can then attempt an incremental send or rollback against a
snapshot that no longer exists on the live destination instead of clearing the
anchor and treating the run as having no current common base.

### Medium: empty cached transfer lists skip the live destination recheck entirely

`src/zxfer_replication.sh` begins
`zxfer_reconcile_live_destination_snapshot_state()` by calling
`zxfer_get_snapshot_transfer_bounds()` and returning immediately when the cached
`g_src_snapshot_transfer_list` is empty. `zxfer_copy_snapshots()` then makes the
same empty-list check and logs `No snapshots to copy`, so zxfer never rechecks
the live destination before skipping the dataset. In a direct helper repro with
`g_last_common_snap=tank/src@base`, an empty cached transfer list, and a live
destination probe that reports the dataset still exists but now has no
snapshots, `zxfer_copy_snapshots` emits only `No snapshots to copy, skipping
destination dataset: backup/target/src.` and never reseeds `tank/src@base`.
That leaves zxfer trusting stale discovery state when the destination loses its
last common snapshot after planning, so a dataset that now needs a bootstrap
receive can be silently skipped as already up to date.

### Medium: cached “already at final snapshot” state can suppress the live recheck after destination loss

`src/zxfer_replication.sh` short-circuits
`zxfer_reconcile_live_destination_snapshot_state()` when the cached
`g_last_common_snap` already points at the final source snapshot, and
`zxfer_copy_snapshots()` then uses the same equality check to return `No new
snapshots to copy` without revalidating the destination. In a direct helper
repro with `g_last_common_snap=tank/src@base` and
`g_src_snapshot_transfer_list=tank/src@base`, `zxfer_copy_snapshots` emits only
`No new snapshots to copy for backup/target/src.` and never reaches the live
destination snapshot probe. If the destination lost that snapshot after
discovery, zxfer still trusts the stale cached equality and skips the dataset
instead of reseeding it. That can silently leave a destination behind while the
run reports nothing to copy.

### Medium: cached “initial destination missing” state can drive the wrong bootstrap path for the root dataset

`src/zxfer_replication.sh` intentionally skips the live existence recheck for
the initial destination root when discovery cached it as missing, but
`zxfer_seed_destination_for_snapshot_transfer()` then reuses the same cached
existence result instead of forcing a live probe before choosing the bootstrap
branch. In a direct helper repro with `g_initial_source=tank/src`,
`g_actual_dest=backup/target/src`, `g_last_common_snap=''`,
`g_src_snapshot_transfer_list=tank/src@base`, and an `zxfer_exists_destination`
stub that returns cached `0` but live `1`, `zxfer_copy_snapshots` calls
`zxfer_zfs_send_receive "" tank/src@base backup/target/src 0` and then declares
the transfer complete without ever issuing the live destination probe. That
lets stale discovery state treat an externally created initial destination as
missing and choose the wrong full-receive path, which can either fail against
an existing snapshotted dataset or mask that zxfer skipped the live revalidation
it needed before bootstrapping.

### Medium: foreground recursive receives leave child destination tracking stale for property reconciliation

`src/zxfer_send_receive.sh` only repairs destination tracking for the exact
receive target by calling `zxfer_note_destination_dataset_exists("$l_dest")`
after a successful foreground receive, even when the received recursive stream
also created descendant datasets. Later, `src/zxfer_property_reconcile.sh`
decides whether a destination dataset exists by grepping `g_recursive_dest_list`
instead of consulting `zxfer_exists_destination()` or the destination-existence
cache. In a direct helper repro, starting from only
`zxfer_note_destination_dataset_exists "backup/dst"` and then running
`zxfer_transfer_properties "tank/src/child"` with
`g_actual_dest=backup/dst/child` drives
`zxfer_run_zfs_create_with_properties yes filesystem "" compression=lz4 backup/dst/child`.
That means a normal foreground recursive receive of the root dataset can leave
child property passes thinking the child still needs `zfs create`, which can
abort against an already-created child dataset instead of diffing and applying
its properties.

### Medium: recursive root receives leave descendant destination-property caches stale

`src/zxfer_send_receive.sh` invalidates destination property state only for the
exact receive target by calling `zxfer_invalidate_destination_property_cache
"$l_dest"` after a successful receive. But
`src/zxfer_property_cache.sh` removes only that one dataset cache entry, so any
prefetched or exact cached child payloads under the recursively received root
remain readable. In a direct helper repro, seeding
`backup/dst/child -> compression=gzip=inherited`, then running
`zxfer_zfs_send_receive "" tank/src@snap1 backup/dst 0`, still leaves the child
cache file present, keeps `g_zxfer_destination_property_tree_prefetch_state=1`,
and makes `zxfer_get_normalized_dataset_properties backup/dst/child /sbin/zfs
destination` return the stale cached child payload without touching `zfs get`.
That means a successful recursive receive of the root dataset can leave later
child property reconciliation diffing against old destination properties for the
rest of the run.

### Medium: recursive root receives leave descendant required-property probe caches stale

`src/zxfer_send_receive.sh` uses the same exact-dataset-only invalidation after
successful receives, and `src/zxfer_property_cache.sh` removes required-property
cache entries only for that one dataset path. Later,
`zxfer_get_required_property_probe()` reads cached destination must-create probe
results before issuing any live `zfs get`. In a direct helper repro, seeding a
cached `casesensitivity=sensitive=local` result for `backup/dst/child`, then
running `zxfer_zfs_send_receive "" tank/src@snap1 backup/dst 0`, still leaves
the child required-property cache file present and
`zxfer_ensure_required_properties_present backup/dst/child "compression=lz4=local" /sbin/zfs casesensitivity destination`
returns `compression=lz4=local,casesensitivity=sensitive=local` without touching
`zfs get`. That means a successful recursive receive of the root dataset can
leave later child must-create backfill logic trusting old destination creation
property values for descendants.

### Medium: recursive root receives leave descendant destination snapshot caches stale

`src/zxfer_send_receive.sh` only records the exact receive target as existing
after success and does not invalidate destination snapshot-record state. Later,
`src/zxfer_snapshot_state.sh` keeps serving destination snapshot lookups from
the existing ready index or stale `g_rzfs_list_hr_snap`, and
`src/zxfer_snapshot_reconcile.sh` trusts those cached child snapshot lists when
deriving `g_dest_has_snapshots`, `g_last_common_snap`, and the child transfer
list. In a direct helper repro, starting from a destination snapshot index that
only contained `backup/dst@base\t111`, then running
`zxfer_zfs_send_receive "" tank/src@base backup/dst 0` and later
`zxfer_inspect_delete_snap 0 tank/src/child` with
`g_actual_dest=backup/dst/child`, left `g_dest_has_snapshots=0`,
`g_last_common_snap=""`, and
`g_src_snapshot_transfer_list=tank/src/child@base\t111`. That means a
successful recursive receive of the root dataset can leave later child snapshot
planning treating an already-received descendant as unsnapped, which can drive
duplicate bootstrap or full-transfer decisions for that child.

### Medium: successful snapshot deletes leave the destination snapshot cache stale

`src/zxfer_snapshot_reconcile.sh` successfully destroys destination snapshots
but does not invalidate `g_rzfs_list_hr_snap` or the ready destination
snapshot-record index afterward. Later, `src/zxfer_snapshot_state.sh` continues
serving destination snapshot lookups from that stale index or cached snapshot
list, and `src/zxfer_snapshot_reconcile.sh` trusts the result when a later
delete-planning pass re-enters `zxfer_inspect_delete_snap()`. In a direct
helper repro, starting from cached destination snapshots
`backup/dst@snap1\t111`, `backup/dst@snap2\t222`, and `backup/dst@snap3\t333`,
one successful `zxfer_delete_snaps` call destroyed `backup/dst@snap3`, but a
subsequent `zxfer_inspect_delete_snap 1 tank/src` still read the stale cached
destination snapshot set and issued `destroy backup/dst@snap3` a second time.
That means successful destination snapshot deletes can leave later planning in
the same run reasoning from snapshots that no longer exist, including retrying
deletes against already-removed snapshots.

### Medium: successful rollback-to-last-common leaves the destination snapshot cache stale

`src/zxfer_replication.sh` rolls the destination back with `rollback -r` after
delete-driven divergence, but it does not invalidate `g_rzfs_list_hr_snap` or
the ready destination snapshot-record index afterward. Later,
`src/zxfer_snapshot_state.sh` continues serving destination snapshot lookups
from that stale cached snapshot set, and `src/zxfer_snapshot_reconcile.sh`
trusts the result when it recalculates `g_last_common_snap` and the remaining
transfer list. In a direct helper repro, starting from cached destination
snapshots `backup/dst@snap1\t111`, `backup/dst@snap2\t222`, and
`backup/dst@snap3\t333`, a successful
`zxfer_rollback_destination_to_last_common_snapshot` to `backup/dst@snap1`
followed by `zxfer_inspect_delete_snap 0 tank/src` still promoted
`tank/src@snap2\t222` as the last common snapshot and left
`g_src_snapshot_transfer_list` empty. That means a successful rollback can
leave later planning in the same run believing a destroyed newer snapshot still
exists on the destination, which can suppress the resend that should restore
that snapshot.

### Medium: cached “destination root missing” state can still hide children after a successful root receive

`src/zxfer_snapshot_state.sh` records a missing destination root by setting
`g_destination_existence_cache_root_complete=1`, which makes uncached
descendants under that root default to `0`. Later,
`zxfer_note_destination_dataset_exists()` only marks the exact dataset and its
parents as present; it does not clear the root-complete missing-subtree
assumption or seed newly created descendants. As a result,
`src/zxfer_exec.sh` can keep returning cached `0` for child datasets without a
live probe even after the root was received successfully. In a direct helper
repro, calling `zxfer_mark_destination_root_missing_in_cache backup/dst`
followed by `zxfer_note_destination_dataset_exists backup/dst` still leaves
`zxfer_exists_destination backup/dst/child` returning cached `0`. That means a
successful recursive receive of the root dataset can leave later cache-based
existence checks treating newly created children as absent until some separate
live probe happens to repair the exact child entry.

### Medium: remote pair backup writes can leave primary and forwarded metadata out of sync after rollback failure

`src/zxfer_backup_metadata.sh` updates the forwarded provenance alias before the
primary exact-pair metadata file, then tries to roll both back if the primary
publish fails. But inside `zxfer_build_remote_backup_pair_write_cmd()`, the
primary rollback restore path exits immediately on a failed `mv` of the primary
rollback file and never calls `rollback_forwarded()`. In a direct helper repro
with the rendered remote helper command, forcing the primary stage publish and
the subsequent primary rollback restore to fail produces exit status `98` while
leaving the primary metadata file missing, the primary rollback file stranded,
and the forwarded alias already updated to the new contents. That can leave
remote `-k` metadata in a split-brain state after an error, where the exact-pair
record and its forwarded provenance alias no longer describe the same transfer.

### Medium: local single-file backup writes can lose the only metadata file when rollback restore fails

`src/zxfer_backup_metadata.sh` uses `zxfer_commit_local_backup_file_stage()` for
plain local backup metadata writes as well as pair writes. When a target file
already exists, that helper first moves the live file to a rollback path and
then tries to publish the staged replacement. If the staged publish fails and
the rollback move back into place also fails, the helper deletes the rollback
file and returns the restore error with no surviving target file. In a direct
helper repro that forced the staged publish and subsequent rollback restore to
fail, `zxfer_write_local_backup_file_atomically` returned `77` with
`state=__MISSING__` and `rollback_count=0`. That means a failed local `-k`
metadata rewrite can leave zxfer with neither the old nor new backup metadata
file, so the only authoritative exact-pair record for that dataset is lost.

### Medium: local pair backup writes can lose the primary metadata file when the primary rollback restore fails

`src/zxfer_backup_metadata.sh` commits the forwarded provenance alias first in
`zxfer_write_local_backup_file_pair_atomically()`, then commits the primary
exact-pair metadata file with `zxfer_commit_local_backup_file_stage()`. If the
primary stage publish fails, `zxfer_commit_local_backup_file_stage()` tries to
restore the old primary file from its rollback path. But when that restore move
fails, it deletes the rollback file and returns the restore error without
re-establishing either the old or new primary file. The caller then rolls the
forwarded alias back successfully and returns the primary restore failure. In a
direct helper repro that forced the primary stage move and the subsequent
primary rollback restore to fail, `zxfer_write_local_backup_file_pair_atomically`
returned `77` with `forwarded=old-forwarded` but `primary=__MISSING__`. That
means a failed local transactional `-k` pair write can still leave the primary
exact-pair metadata file missing even though zxfer already restored the
forwarded alias, so the metadata pair no longer has any complete authoritative
record for that transfer.

### Medium: exact-pair backup metadata filenames can collide across distinct dataset pairs

`src/zxfer_backup_metadata.sh` names exact-pair backup metadata files with the
source tail plus a `cksum`-derived key over `source\ndestination`. That key is
only a 32-bit CRC plus the input length, so distinct dataset pairs can map to
the same filename and silently overwrite each other’s metadata when the source
tail also matches. For example, `tank/lixntn/src -> backup/0135l2/src` and
`tank/cp4hgv/src -> backup/8pnm4u/src` both resolve to the same filename
`.zxfer_backup_info.src.k1796141117.33`. A later restore or chained-backup
lookup can then read the wrong property record for a different dataset pair.

### Medium: property serialization drops user-property values containing line feeds

`src/zxfer_property_cache.sh` serializes property records by reading one
tab-delimited line at a time and percent-encoding a fixed set of characters in
the value, but it never encodes `\n`. OpenZFS documents user-property values
as arbitrary strings, so an embedded line feed in a user property splits the
single `property\tvalue\tsource` record into multiple physical lines before
zxfer reaches the encoder. In a direct helper repro,
`printf "user:note\tline1\nline2\tlocal\n" | zxfer_serialize_property_records_from_stdin`
emits an empty serialized payload. That turns a valid property into silent
loss anywhere zxfer relies on the serialized-property pipeline for cache,
backup, diff, or restore flows.

### Medium: remote capability cache keys can alias distinct host specs and reuse the wrong cached probe

`src/zxfer_remote_hosts.sh` keys the on-disk remote capability cache with a
32-bit `cksum` plus input length and does not store or verify a second
identity inside the cache file on read. Distinct host specs can therefore map
to the same cache path and silently reuse each other’s cached capability
payload within the TTL window. In a direct helper repro with a fixed cache
identity, `host-e00sy5` and `host-entjr8` both resolve to the same cache key
`852947723.26`, and `zxfer_read_remote_capability_cache_file host-entjr8`
returns the payload previously written for `host-e00sy5`. That can make zxfer
trust the wrong remote OS or resolved helper paths for a different host spec.

### Low: fallback `ZXFER_ERROR_LOG` lock keys can alias distinct log paths

`src/zxfer_reporting.sh` derives fallback error-log lock directories from a
32-bit `cksum` of the log path and does not include a second identity in the
lock metadata. When zxfer appends to an existing `ZXFER_ERROR_LOG` under a
trusted but non-writable parent, unrelated log paths can therefore share the
same fallback lock directory under `/tmp` or `/dev/shm`. In a direct helper
repro, `/var/log/zxfer-3kzpfymt.log` and `/var/log/zxfer-amu2x4ex.log` both
resolve to `/tmp/.zxfer-error-log.lock.k2445900035`. That can make unrelated
error-log appends block each other or reuse each other’s stale fallback lock.

### Low: invalid `-o` property names are reported as syntax errors instead of missing-property errors

`src/zxfer_property_reconcile.sh` distinguishes override syntax failures from
missing source properties inside the awk validator, but the shell wrapper
collapses both cases into the same `Invalid option property - check -o list
for syntax errors.` usage error. The run still fails closed, but the current
message sends operators toward quoting/syntax debugging even when the real
problem is that the requested override property does not exist on the source.

### Low: OpenZFS-on-macOS property reconciliation is still not trusted at the FreeBSD/Linux level

`tests/run_integration_zxfer.sh` still skips child `atime=off` assertions on
Darwin, and `docs/platforms.md` plus `docs/testing.md` explicitly describe
Darwin/OpenZFS property behavior as less deterministic than the primary
FreeBSD/Linux validation path. Current releases therefore still have an open
platform-correctness gap for some inherited child-dataset property assertions
on macOS.

### Low: Remote `-O ... -j > 1` only checks that `parallel` exists, not that it is GNU Parallel

`src/zxfer_snapshot_discovery.sh` validates local `-j` helpers as GNU Parallel,
but on remote-origin runs it now accepts any resolved origin-host `parallel`
path and assumes GNU-compatible behavior. The integration harness keeps a
`remote_non_gnu_parallel_origin_test` that expects the run to fail later inside
the rendered discovery pipeline. This fails closed, but it is still an active
compatibility issue because the incompatibility is discovered only after remote
bootstrap rather than during upfront validation.

### Low: Recursive `-o` overrides still flatten inheritance across descendant datasets

The current recursive override semantics intentionally preserve the requested
effective value by explicitly setting descendant properties when needed, and
the man pages document that these properties do not currently remain
inherited-only from the replicated root filesystem. This remains an operator-
visible semantics issue for administrators who expect `-o` on recursive runs
to keep a clean inheritance tree instead of leaving explicit local child
properties behind.

## Architectural Remediation Themes

The current issue inventory suggests a few repeatable failure classes that are
likely worth fixing with shared architectural changes instead of one-off local
patches.

### Suggestion: replace ad hoc property-string rewriting with a typed property plan pipeline

Several issues come from treating property state as comma-separated shell
strings and applying stage-specific text transforms in multiple places. A
single property-plan pipeline that parses records once, stores them in a lossless
staged format, and then derives explicit `create`, `set`, `inherit`, and
`skip-unsupported` outputs would address a large class of bugs at once. That
theme covers the `readonly=on` substring rewrite issue, regex-driven property
filter mistakes, silent line-feed loss in serialized values, the current `-U`
gap between override and creation lists, and the broader drift between
create-time and apply-time property handling. The important design constraint is
that property names and values need exact literal semantics all the way through
the pipeline rather than regex or delimiter-sensitive shell text semantics.

### Suggestion: replace scattered mutable replication caches with one authoritative destination-state refresh layer

Several medium-severity replication issues stem from cached globals such as
`g_last_common_snap`, `g_src_snapshot_transfer_list`, destination-existence
cache entries, and `g_recursive_dest_list` being updated opportunistically and
then reused across later decisions without one consistent live-revalidation
boundary. A stronger design would centralize destination state into one
authoritative refresh step per dataset subtree, with explicit invalidation after
receives, creates, rollbacks, and background-job completion. That should let
planning and property reconciliation read one coherent state snapshot instead of
mixing stale per-helper caches. This theme would address the stale common-base
and skipped live-recheck replication bugs, the wrong bootstrap-path issue, and
the foreground/background receive cases where child existence tracking falls out
of sync.

### Suggestion: replace ambient shared-global coordination with explicit staged contracts between phases

Many of the current bugs only become possible because helpers communicate by
mutating shared globals and then expecting later code to remember which of
those values are authoritative, filtered, invalidated, or only provisional for
the current phase. That shows up in replication planning, property-create
decisions, background receive follow-up, and backup-metadata publish/rollback
flows. A stronger design would make each phase emit one explicit staged
contract for the next phase: for example a validated destination-state bundle,
a property action plan, or a transactional publish state record, instead of
relying on ambient `g_*` variables that several modules mutate in place. That
would reduce cross-phase state bleed, make partial refresh/invalidation rules
more obvious, and narrow the number of places where one stale or half-updated
global can silently redirect later destructive decisions.

### Suggestion: separate authoritative inventories from filtered worklists and exact-create notes

Several issues come from one mutable shell list doing double duty as both a
complete subtree inventory and a scratch worklist that later code keeps
mutating. `g_recursive_dest_list`, the destination-existence cache root marker,
and other “known destination” state are sometimes treated as authoritative
complete inventories, but later flows append only exact datasets, preserve old
negative-subtree assumptions, or filter the original discovery results for
action selection. A stronger design would keep three distinct concepts
separate: the last authoritative discovered inventory, the current incremental
mutation journal for this run, and filtered action/work queues derived from
those sources. That would address the repeated child-existence and
already-created-dataset bugs after foreground/background receives, the cached
“root missing” subtree bugs, and the broader class of create/diff decisions
that currently depend on whether one mutable newline list happened to be
complete.

### Suggestion: replace exact-dataset cache invalidation with mutation-scope and generation-based invalidation

Many of the stale-cache bugs are not just “missing refresh” problems; they come
from invalidating only the exact dataset that was mutated even when the
operation changes descendant state or invalidates a derived subtree-wide view.
Parent property changes can alter inherited child properties, recursive
receives can invalidate descendant property, required-property, and snapshot
caches, and snapshot deletes or rollbacks can invalidate every later lookup
derived from the cached destination snapshot index. A stronger design would tag
cached objects with a subtree generation or mutation scope and bump that scope
after receives, create paths, deletes, rollbacks, and parent-level property
changes. That would address the repeated descendant-staleness issues without
requiring every caller to remember each exact cache family that a given
mutation should invalidate.

### Suggestion: centralize namespace mapping with prefix-aware dataset and snapshot identity helpers

zxfer still performs several source-to-destination and destination-to-source
translations as ad hoc shell string rewrites. That is brittle when names repeat
path fragments, when descendants are mapped relative to a root dataset, or when
later code needs to preserve both a dataset path and a snapshot identity at the
same time. A shared namespace-mapping layer that works on parsed
dataset/snapshot objects and only rewrites validated leading prefixes would
address the repeated-prefix destination snapshot normalization bug and reduce
the risk of future path-corruption or mismatched-identity planning errors in
recursive discovery, delete planning, and backup-metadata lookup paths.

### Suggestion: replace scattered helper and platform heuristics with one verified capability registry

Some remaining issues are really compatibility-contract problems: zxfer still
relies on a mix of one-off banner checks, local assumptions, and partial
behavior probes when deciding whether a platform or helper is safe to trust.
The GNU Parallel compatibility gap for remote `-O ... -j > 1`, the current
OpenZFS-on-macOS property-reconciliation trust gap, and several
dataset-type/property-semantics branches all point in the same direction. A
shared capability registry populated by explicit feature probes and keyed by
host role, platform, OpenZFS flavor, and helper identity would let later code
consume one authoritative “can this host safely do X?” answer instead of
re-deriving behavior from ad hoc shell tests. That would reduce fail-open
compatibility drift and make platform-specific degradations visible in one
place instead of being scattered through replication and property code paths.

### Suggestion: replace 32-bit `cksum`-derived identities with collision-resistant keys plus embedded identity verification

Multiple issues come from naming cache or metadata artifacts with only a 32-bit
`cksum` and length. A shared runtime helper that derives artifact paths from a
collision-resistant digest and also stores the original logical identity inside
the file or lock metadata would remove that class of aliasing bug across the
codebase. That applies directly to exact-pair backup metadata filenames, remote
capability cache files, and fallback error-log locks. Even where a collision is
only causing lock contention instead of wrong-data reads, the underlying design
problem is the same weak keying scheme.

### Suggestion: introduce shared transactional publish primitives

The remaining issue cluster here is around multi-step state publication rather
than runner identification. Backup metadata updates still roll several files
forward and backward with hand-built sequencing, and a shared transaction
helper for multi-file publish/rollback would reduce the risk of split-brain
metadata while making lifecycle state more explicit.
