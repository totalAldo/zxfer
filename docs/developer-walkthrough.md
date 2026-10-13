# Developer Walkthrough

Start with one ordinary incremental replication, then follow the branches your
change affects. [Architecture](./architecture.md) documents the complete module
map and state ownership; [data formats](./data-formats.md) explains the records
used below.

The three orchestration lifetimes are **session → pass → dataset**:
`zxfer_main` prepares and releases the session, `zxfer_run_zfs_mode` coordinates
one pass, and `zxfer_process_source_dataset` processes a ready dataset. Start
at the entry point for the lifetime your change affects, then follow its domain
helpers.

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
   CLI prepares local codecs after parsing, before any SSH connection.
   `zxfer_resolve_cli_command_safe` in remote hosts resolves a command head
   locally or remotely and quotes its arguments. Dependency lookup and path
   validation support that operation without calling remote coordination.
   Session's endpoint initializer publishes the resolved OS, zfs command and
   codec only after the endpoint's required lookups succeed. It uses the local
   context when no remote host is configured and skips live probes in dry runs.
2. **Discover completed state.** `zxfer_run_zfs_mode_loop` in
   [replication](../src/zxfer_replication.sh) starts a pass.
   `zxfer_initialize_replication_context` obtains source and destination state
   through [snapshot discovery](../src/zxfer_snapshot_discovery.sh).
   Discovery results are validated before planning uses them. A failed or
   incomplete listing must not become permission to delete or send.
   [Destination state](../src/zxfer_destination_state.sh) retains the validated
   destination inventory once. Later existence observations override membership;
   only a complete inventory supplies the default that unlisted datasets are
   missing. Receive completion clears that default for live descendant probes.
   On SunOS, ambiguous existence failures walk parent listings toward the pool
   in one loop, retaining the first parent's diagnostic if absence stays unproven.
   The fast proof has one owner, `zxfer_try_fast_recursive_noop_discovery`,
   which allocates, overlaps and reaps producers, validates, publishes and
   releases scratch. `zxfer_run_full_snapshot_discovery` owns the full operation in the same way
   and reuses a successful destination listing when the proof declines.
   Each owner keeps one workspace stem and its allocated path list; names such
   as `STEM.error` and `STEM.raw` identify each file's purpose. Delta processing
   extracts the transfer, extra and optional full-source dataset lists together,
   sorts them once by role and dataset, then publishes each list in the shell.
   Its destination-inventory decision also governs snapshot-record retention;
   nonrecursive runs retain records even when no inventory is needed. Parallel
   accounting follows the listing mode: the proof ignores `-j`, and full
   discovery uses parallel when `-j` exceeds one. With `-s` or `-m`, replication coordinates the
   first pass's preparation and rediscovery here, before the dataset queue;
   migration services owns only service stopping and recovery.
3. **Visit each dataset.** `zxfer_copy_filesystems` builds the iteration list
   and prepares its snapshot plans in one grouped traversal before passing it
   to the ready queue. The `-g` preflight loads the snapshot-work subset in its
   original order; execution reuses those plans. Planner memory holds only one
   dataset's snapshots, and source slices remain available for live rechecks.
   `zxfer_process_source_dataset` maps the destination and selects that dataset's
   prepared snapshot plan. With parallel sends,
   the queue also prevents conflicting destination ancestry from running at
   the same time; it is not simply a collection of independent background jobs.
4. **Plan snapshots.** `zxfer_inspect_delete_snap` in
   [snapshot planning](../src/zxfer_snapshot_plan.sh) loads the prepared
   name-and-GUID comparison as a common snapshot and first/final pending records.
   Exact pending identities and ordered deletions stay in the private plan file;
   an ordered whole-line summary ends with a checked completion marker, so
   loading needs no AWK process. The divergence summary retains a count and up
   to three examples. A GUID-less row is recorded during preparation
   and refused here with its original status; `-g` refuses it during preflight.
   Here `@one` is the shared anchor and `@two` needs transfer. Destination
   deletion is a separate, guarded action. A same-named snapshot with a
   different GUID is divergence, not an incremental anchor.
   Live re-planning consumes the retained listing file only after a successful
   listing. Runtime owns file allocation and reuse; the dataset modules retain
   the paths and decide when their contents can be used.
5. **Reconcile properties.** `zxfer_transfer_properties` in
   [property transfer](../src/zxfer_property_transfer.sh) reads source state,
   obtains destination state, builds one batched plan, and applies allowed
   changes. The dataset operation retains its original and effective source
   views. Planning takes explicit root and destination-exists flags, dataset
   type, both property views and the type-specific unsupported list; run-wide
   `-P`, `-o`, readonly and `-I` policy is read from prepared session state.
   Existence never depends on the destination list being nonempty or on the
   number of arguments. Property reads reuse one workspace: the stem holds the
   property-name skeleton, with `.machine`, `.human`, `.error` and `.wanted`
   siblings for the value views, diagnostics and recursive dataset filter.
   One AWK consumes staged encoded inputs and writes selected actions
   into a reusable private file, keeping values out of the command environment.
   The operation validates the file before mutation, resolves child inheritance
   against the parent after earlier changes, then consumes the plan for create,
   one batched set and ordered individual inherits. Full diagnostic lists are
   built only when verbose output needs them. The planner keeps one ordered
   encoded record list and first-value/provenance maps, with explicit root and
   child decisions; all creation-time constraints are checked before actions
   are emitted. Repeated restored records keep their order and distinct values.
   Here it sets `compression=lz4`; an equal property needs no write.
   Readonly rules and creation-time constraints still apply. With `-k`, it
   also buffers original source property metadata, separately from overrides.
   Backup restore and forwarded provenance share
   `zxfer_load_backup_metadata_view`: guarded reads stay in private staging,
   validation keeps keys and counts, and payload rows are written after the
   file passes validation. Dataset lookups use the shared row reader.
   Restore views refresh per pass; alias views survive for the session.
6. **Transfer snapshots.** `zxfer_copy_snapshots` chooses the seed or
   incremental path and delegates transport to
   [send/receive](../src/zxfer_send_receive.sh). `zxfer_zfs_send_receive`
   prepares and dispatches the pipeline; the same module owns job limits,
   ancestry serialization, completion and teardown. Here it sends from the shared
   anchor to the newest pending snapshot. An incremental stream can include
   intermediate snapshots through `zfs send -I`; only the range endpoints are
   needed in shell state. After deletions, a live anchor must exactly match the
   original common or a pending name/GUID record retained in the plan file. Both sides' failures must propagate through the transport.
   For an SSH hop, the transport dispatcher owns host parsing, wrapper prefixes
   and argv assembly. Wrapper-aware preparation encloses the remote pipeline
   in `sh -c`; wrapping publishes the complete command, including its local
   compression stage, in `g_zxfer_shell_command_result`. Send/receive retains
   each side before preparing the other side or the progress stage.
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
For example, dependency validation and lazy SSH lookup publish a path or
diagnostic through `g_zxfer_required_tool_result`. SSH retains its command in
`g_cmd_ssh` and republishes it on a cache hit. Consume the result before another
lookup; command-head resolution publishes a quoted command with arguments in
the separate `g_zxfer_resolved_cli_command_result`.

Remote `sh -c` programs, wrapped transfers and backup dry-run previews all
publish `g_zxfer_shell_command_result`. Builders retain an input passed through
that channel before rewriting it; callers retain a successful result before
another command is prepared. A failed wrapper publishes no command and keeps
the original failure status. These results have no session reset or cache.

Source listing builders use the same canonical command result. Launchers retain
it before preparing producers; `g_source_snapshot_list_cmd` keeps the launched
command until producer validation can report it. Failed builders preserve the
existing diagnostic or empty result and status, clearing partial render output.

Property table and restore lookups publish the row reader's
`g_zxfer_property_row_payload_result` directly. The property operation retains
the raw source view and effective restore view before another helper can replace
that result; the two views have different purposes and lifetimes.
Default temp-directory selection similarly publishes the path validator's
`g_zxfer_temp_root_candidate_result`; the effective TMPDIR memo retains the
selected path for the session. Multiple artifacts use the existing batched
newline-list cleanup, with whole-root cleanup at exit.

POSIX shell functions do not make `l_*` variables local. The prefix communicates
intent, and distinct names prevent nested helpers from overwriting scratch
state. Function positional parameters provide function-scoped argument storage.
Migration stop and restore use them for literal service names, and the restart
queue itself determines whether exit recovery has work. A separate guard keeps
exit from repeating an ordinary restore that already failed.
Do not hide safety checks or move work into command substitutions solely to
reduce the number of globals: subshell state and extra processes change the
execution contract.
