# Developer Walkthrough

Start with one ordinary incremental replication, then follow the branches your
change affects. [Architecture](./architecture.md) documents the complete module
map and state ownership; [data formats](./data-formats.md) explains the records
used below.

## One Operation

Imagine a recursive run with `-R`, property transfer with `-P`, and property
backup with `-k`. The dataset names below are illustrative:

| Input | State |
| --- | --- |
| Source root | `tank/src`, without a trailing slash |
| Destination argument | `backup/dst` |
| Mapped destination root | `backup/dst/src` |
| Source snapshots | `@one` with GUID `101`, then `@two` with GUID `102` |
| Destination snapshots | `@one` with GUID `101` |
| Source property | `compression=lz4`, locally set |
| Destination property | `compression=gzip`, locally set |

There is no destination deletion option in this example. Children follow the
same relative mapping, and each dataset gets its own snapshot and property plan.

1. **Initialize and validate.** The [launcher](../zxfer) loads modules in the
   order declared by [zxfer_modules.sh](../src/zxfer_modules.sh).
   `zxfer_main` in [session](../src/zxfer_session.sh) initializes run state,
   cleanup traps, trusted helpers, and private temporary storage, then calls
   `zxfer_session_run`. That function parses and validates the CLI, prepares
   remote connections when requested, and initializes endpoint policy.
2. **Discover completed state.** `zxfer_run_zfs_mode_loop` in
   [replication](../src/zxfer_replication.sh) starts a pass.
   `zxfer_initialize_replication_context` obtains source and destination state
   through [snapshot discovery](../src/zxfer_snapshot_discovery.sh).
   Discovery results are validated before planning uses them. A failed or
   incomplete listing must not become permission to delete or send.
3. **Visit each dataset.** `zxfer_copy_filesystems` builds the iteration list
   and passes it to the ready queue. `zxfer_process_source_dataset` maps the
   destination and selects that dataset's snapshot slice. With parallel sends,
   the queue also prevents conflicting destination ancestry from running at
   the same time; it is not simply a collection of independent background jobs.
4. **Plan snapshots.** `zxfer_inspect_delete_snap` in
   [snapshot planning](../src/zxfer_snapshot_plan.sh) compares names and GUIDs.
   Here `@one` is the shared anchor and `@two` needs transfer. Destination
   deletion is a separate, guarded action. A same-named snapshot with a
   different GUID is divergence, not an incremental anchor.
5. **Reconcile properties.** `zxfer_transfer_properties` in
   [property transfer](../src/zxfer_property_transfer.sh) reads source state,
   obtains destination state, builds one batched plan, and applies allowed
   changes. Here it sets `compression=lz4`; an equal property needs no write.
   Readonly rules and creation-time constraints still apply. With `-k`, it
   also buffers original source property metadata, separately from overrides.
6. **Transfer snapshots.** `zxfer_copy_snapshots` chooses the seed or
   incremental path and delegates transport to
   [send/receive](../src/zxfer_send_receive.sh). Here it sends from the shared
   anchor to the newest pending snapshot. An incremental stream can include
   intermediate snapshots; the transfer list does not mean one process per
   snapshot. Both sides' failures must propagate through the transport.
7. **Finish and release.** The queue waits for send jobs. Seeded datasets get
   another property reconciliation after receives finish, because a seed can
   create a destination without the desired properties. Buffered `-k` metadata
   is published at the post-seed checkpoint and at run end by
   [backup metadata](../src/zxfer_backup_metadata.sh). `zxfer_trap_exit` in
   session coordinates job and helper shutdown, SSH teardown, and release of
   private temporary storage through [runtime](../src/zxfer_runtime.sh), while
   preserving failure status.

For a missing destination, follow the seed branch in step 6: seed the oldest
pending snapshot before subsequent incremental work. For an identical tree,
discovery may prove that no snapshot transfer is needed; requested property
work still has its own decision path. `-n` skips live snapshot discovery and
does not render a complete send/receive or property reconciliation plan.

## Observe the Flow Without ZFS

Run these commands from the repository root. The contract suites exercise the
real launcher with mock commands and file-backed fixtures; they do not use host
pools or remote hosts.

```sh
./tests/run_shunit_tests.sh --suite tests/test_contract_planning.sh \
  --test test_recursive_discovery_pins_guid_identity_listings \
  --test test_identical_source_and_destination_is_a_proven_noop \
  --test test_property_pass_sets_differing_property_and_skips_identical_one \
  --test test_missing_destination_child_is_created_with_creation_properties_before_receive

./tests/run_shunit_tests.sh --suite tests/test_contract_send_receive.sh \
  --test test_post_seed_property_pass_reconciles_every_seeded_dataset_after_the_receives
```

Read those tests alongside the corresponding step. In
[test_contract_planning.sh](../tests/test_contract_planning.sh), `ZFS_LOG`
assertions show discovery arguments, property writes, and transfer ordering.
The fixture helpers supply starting state, and teardown removes temporary test
state. These focused tests explain individual contracts rather than reproducing
the whole illustrative operation in one test.

## Follow State by Lifetime

Read a module's `Module contract` header before following its globals. Separate
validated session policy, per-pass caches, operation resources, and short-lived
result channels. A result channel should be consumed according to the helper's
contract; it is not another durable copy of session state.

POSIX shell functions do not make `l_*` variables local. The prefix communicates
intent, and distinct names prevent nested helpers from overwriting scratch
state. Function positional parameters provide function-scoped argument storage.
Do not hide safety checks or move work into command substitutions solely to
reduce the number of globals: subshell state and extra processes change the
execution contract.
