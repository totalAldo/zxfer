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

The position connects a row to its prepared snapshot slice. Dataset ordering
puts parents before children; the parallel ready queue additionally gates
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

[Snapshot planning](../src/zxfer_snapshot_plan.sh) writes a tagged plan file.
Its record types are:

| Tag | Remaining fields | Meaning |
| --- | --- | --- |
| `common` | Snapshot record | Newest shared source snapshot |
| `diverged` | Name, source GUID, destination GUID | Conflicting identity |
| `send` | Snapshot record | Pending source snapshot, oldest first |
| `delete` | Destination snapshot path | Candidate requiring deletion guards |
| `destination_present` | `0` or `1` | Validated destination snapshot presence |
| `sources` | Count | Final record proving the plan completed |

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

[Property transfer](../src/zxfer_property_transfer.sh) computes one batch for
a dataset. Its successful output contains six property-list lines, in order:

1. Apply list, including effective overrides.
2. Creation list.
3. Filtered destination list.
4. Initial set list.
5. Child set list.
6. Inherit list.

The next line is `__ZXFER_PROPERTY_PLAN__`, followed by any warning lines.
Empty list lines are meaningful; do not drop them or split the batch on shell
whitespace. The planner validates completion before publishing its result
channel, and the dataset operation consumes the lists before applying them.
A creation-time mismatch takes a separate error path, not a partial success.

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
Metadata preserves source properties, including provenance, rather than only
the effective values applied through `-o`. The forwarded copy adjusts the
source root for the next replication hop.

Format validation complements filesystem security: private paths, ownership,
link checks, recovery, and atomic publication remain the metadata module's
responsibility. Keep readers and writers together when reviewing a format
change. See [SECURITY.md](../SECURITY.md) for the surrounding trust boundaries.
