# Data Formats for Developers

This guide covers the representations used along the
[developer walkthrough](./developer-walkthrough.md). Internal records are not
shell command text. Keep their delimiters, ordering, validation, and quoting
contracts when changing a producer or consumer.

In examples, `<TAB>` denotes one literal tab and `<LF>` one newline. They are
display notation, not characters to store. Dataset and snapshot names are
illustrative. Internal formats may evolve with their consumers; the on-disk
backup format has an explicit version and compatibility contract.

## Dataset Mapping and Iteration

[Destination state](../src/zxfer_destination_state.sh) owns
`zxfer_map_destination_dataset`. The source argument's trailing slash decides
whether the source root's final component is appended to the destination:

| Source argument | Destination argument | Mapped root | Mapped child |
| --- | --- | --- | --- |
| `tank/src` | `backup/dst` | `backup/dst/src` | `backup/dst/src/child` |
| `tank/src/` | `backup/dst` | `backup/dst` | `backup/dst/child` |

The replication iteration list uses `POSITION<TAB>SOURCE` rows:

```text
1<TAB>tank/src
2<TAB>tank/src/child
```

The position connects a row to its prepared snapshot plan and source slice.
Dataset ordering puts parents before children; the parallel ready queue additionally gates
conflicting destination ancestry. Do not replace these rows with shell words
or assume that a row's position alone makes it safe to start a receive.

## Snapshot Identity and Plans

[Snapshot discovery](../src/zxfer_snapshot_discovery.sh) produces records shaped
as `dataset@snapshot<TAB>guid`:

```text
tank/src@two<TAB>102
tank/src@one<TAB>101
```

This example is in the newest-first order consumed by the dataset planner.
Discovery also works with creation-ordered listings; ordering is part of each
helper's contract, not a property of the record syntax. A common snapshot must
match both name and GUID on the mapped datasets. Same name with a different
GUID is divergence; a missing GUID is an invalid identity.

Discovery's temporary dataset rows are `ROLE<TAB>DATASET`, where `ROLE` is
`missing`, `extra`, or `inventory`. One C-locale unique sort orders and
deduplicates each role independently; the same dataset may belong to several
roles. The published globals remain newline-separated dataset lists. These
rows schedule inspection; they do not validate GUIDs or authorize deletion.

[Snapshot planning](../src/zxfer_snapshot_plan.sh) writes exact identity and
deletion rows, followed by a fixed-order summary. Its tagged rows are:

| Tag | Remaining fields | Meaning |
| --- | --- | --- |
| `common` | Snapshot record | Newest shared source snapshot |
| `diverged` | Name, source GUID, destination GUID | Conflicting identity |
| `send` | Snapshot record | Pending source snapshot, newest first |
| `skip` | `before_common`, snapshot record | Under `-v`/`-V`, missing identity before the common snapshot in source creation order; diagnostic only |
| `delete` | Destination snapshot path | Candidate requiring deletion guards, in destination order |
| `error` | `3` | Invalid identity, refused at this dataset’s inspection boundary |

The summary starts with `__ZXFER_SNAPSHOT_SUMMARY__`, then has six whole-line
values: first pending record (oldest), final pending record (newest), the single
reverse-order destroy argument, destination presence (`0` or `1`), source
count, and divergence count. Empty endpoints or destroy targets occupy empty
lines; tabs inside snapshot records remain literal. The block ends with
`__ZXFER_SNAPSHOT_PLAN__` and a newline, with no following data. For example:

```text
send<TAB>tank/src@two<TAB>102
common<TAB>tank/src@one<TAB>101
__ZXFER_SNAPSHOT_SUMMARY__
tank/src@two<TAB>102
tank/src@two<TAB>102

1
2
0
__ZXFER_SNAPSHOT_PLAN__
```

Prepared plans live in `BASE.POSITION.p`; `BASE.POSITION.s` retains source
records for live rechecks. The shell loads summaries without another AWK
process, rejecting missing values, invalid counts or presence, wrong completion
markers, and trailing data. An identity failure ends with `error` instead of a
summary. Preflight and execution read the same plans. No plan is published when
the grouped preparation fails, and loading a draft never changes the active
transfer plan. Verbose-only `skip` rows are reported after divergence
enforcement during dataset execution, so `-g` preflight does not duplicate
them. They never authorize sends, deletes or adoption of an older live anchor.

Fields are tab-separated, including the name and GUID inside a snapshot
record. A delete candidate is not authorization to delete: option checks,
retention protection, and execution checks still govern the operation.

## Property Lists and Row Storage

[Property state](../src/zxfer_property_state.sh) represents a property list as
comma-separated `property=value=source` items:

```text
compression=lz4=local,user:note=a%3Db%2Cc%3B%25=local
```

The second value decodes to `a=b,c;%`. Property values encode the following
characters so they cannot be mistaken for record boundaries:

| Character | Encoding |
| --- | --- |
| `%` | `%25` |
| `,` | `%2C` |
| `=` | `%3D` |
| `;` | `%3B` |
| Tab | `%09` |
| Carriage return | `%0D` |
| Newline | `%0A` |

The source field carries provenance, such as `local` or
`inherited from tank/src`. Equal values can still require different actions
when provenance differs. Reuse the existing encoding and decoding helpers;
decode `%25` last so a literal encoded-looking value is not decoded twice.
An encoded list still needs normal argv quoting before any command execution.

Each property list occupies one private row file. Source and destination tables
are small indexes into that shared row store, with newest entries first:

```text
<LF>p2<TAB>tank/src/child<LF>p1<TAB>tank/src<LF>
```

`p1` and `p2` name row files, not property values. The first matching dataset
entry wins. A row name of `-` is a tombstone: lookup misses even if an older
entry exists. Empty or unreadable rows also hide older entries. Receive
invalidates that dataset's property entry; create, set, and inherit also
invalidate descendants because inherited values can change. Restore rows use
the same storage with an `r` prefix.

## Batched Property Plan

The internal entry point is
`zxfer_plan_property_changes IS_INITIAL HAS_DESTINATION TYPE SOURCE_PVS DEST_PVS UNSUPPORTED_CSV`.
It requires six arguments and two explicit 0/1 flags. `HAS_DESTINATION=1`
means diff against the supplied destination view, including an empty one;
0 means plan creation. Session startup prepares the run-wide `-P`, `-o`,
readonly and `-I` policies that this function reads. The dataset-specific
unsupported list remains a parameter because filesystem and volume policies
differ. This replaces the former nine-argument internal signature; operator
flags, outputs, action records and backup format v2 are unchanged.

[Property transfer](../src/zxfer_property_transfer.sh) computes one batch for
a dataset. Its inputs are encoded lists in one reusable run-private file,
with `KEY<TAB>LIST` rows and a final `__ZXFER_PROPERTY_INPUT__` line. Keys
name the views (`source`, `override`, `destination`); late parent resolution
uses `parent`. Name-only filter policies and
scalar flags remain explicit command context. Empty values remain rows. The reader rejects malformed or repeated
keys, a missing completion marker, and trailing data. Values cross no shell
or AWK escape parser, and the file is fully consumed before it can be reused.
The capture parser and cached row format are unchanged.

The output is a private action file, not a batch of shell property lists:

```text
<create|set|inherit><TAB><property>=<encoded value>=<source>=<override desired value>
__ZXFER_PROPERTY_ACTIONS__
```

The fourth field is empty unless the source is `override`. It retains the
first desired value for late parent reconciliation. Ordered encoded records
retain repeated properties and their distinct values; the first record supplies
their provenance. The planner checks every creation-time constraint before
emitting actions. Separate root and child decisions select only the current
dataset's actions. With `-V`, additional `view<TAB>name=LIST` rows retain the
diagnostic views of both decisions; ordinary runs omit those rows.

For `compression=lz4=local` on the source and `compression=gzip=local` on the
destination, the plan is:

```text
set<TAB>compression=lz4=local=
__ZXFER_PROPERTY_ACTIONS__
```

Application includes `compression=lz4` in the dataset's single `zfs set` call.
A child's `inherit` action is resolved against its current destination parent
before application; a parent that does not provide the required value can
turn that action into a `set`. Sets run as one command, followed by individual
inherits in their planned order. Values are decoded only when constructing argv.

The planner prints `__ZXFER_PROPERTY_PLAN__` and any warnings on stdout after
closing the file successfully. The shell validates the complete action file
before publishing action/override counts or applying it, rejecting missing
markers, malformed records and trailing data. A valid empty plan contains only
the final marker. A creation-time mismatch retains its separate error path.

## Property Backup Metadata

[Backup metadata](../src/zxfer_backup_metadata.sh) owns format version 2 and
its validation and publication. A shortened excerpt looks like:

```text
#zxfer property backup file
#format_version:2
#source_root:tank/src
#destination_root:backup/dst/src
.<TAB>compression=lz4=local
child<TAB>compression=lz4=inherited from tank/src
```

The writer also records zxfer version, recursive options, and backup date.
Rows use a relative dataset key and the same encoded property-list syntax.
`.` means the root; `child` means the same relative child under both roots.
Restore checks the root mapping and required headers before using a row.
The private read view contains a compact key-to-row index and individual
payload files; duplicate keys use a tombstone and fail when selected. Both
restore and forwarding consume this view without changing the on-disk format.
Metadata preserves source properties, including provenance, rather than only
the effective values applied through `-o`. The forwarded copy adjusts the
source root for the next replication hop.

Format validation complements filesystem security: private paths, ownership,
link checks, recovery, and atomic publication remain the metadata module's
responsibility. Keep readers and writers together when reviewing a format
change. See [SECURITY.md](../SECURITY.md) for the surrounding trust boundaries.
