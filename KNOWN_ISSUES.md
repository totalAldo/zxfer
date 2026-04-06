# KNOWN ISSUES

This file tracks open issues that still matter for current releases. Resolved
historical bugs should be removed instead of accumulated here.

## Open Functional Issues

- Remote OS detection still goes through `eval` and the remote login shell's default `PATH`.
  Files: `src/zxfer_common.sh` (`get_os()`), `src/zxfer_globals.sh` (`init_variables()`).
  Impact: remote platform detection can execute the remote account's `uname` instead of a securely resolved binary, and shell behavior differences remain in play earlier than the hardened remote `zfs`/`cat` lookup paths. This mostly affects feature gating and platform-specific property handling, not the main replication stream.
  Recommended fix: replace `get_os()` with the same argv-based ssh helper path used by `run_source_zfs_cmd()` / `run_destination_zfs_cmd()`, or resolve `uname` remotely through the secure PATH before use.

- Remote `zfs send` / `zfs receive` still shell out to the local `zfs` path when `-O` or `-T` is used.
  Files: `src/zxfer_zfs_send_receive.sh` (`get_send_command()`, `get_receive_command()`, `zfs_send_receive()`), `src/zxfer_globals.sh` (`init_variables()`).
  Impact: zxfer now probes and stores remote `zfs` paths in `g_origin_cmd_zfs` / `g_target_cmd_zfs`, but the main replication stream still builds send/receive commands with `g_cmd_zfs`. Mixed-platform or mixed-layout hosts can therefore pass dependency validation and snapshot discovery, then fail during the actual replication stream because the remote host does not have `zfs` at the local absolute path.
  Recommended fix: build send/receive commands from the already resolved remote `zfs` paths (or from argv-based source/target command helpers) instead of hard-coding `g_cmd_zfs` into the stream command string.

- Remote compression still depends on bare remote `zstd`, and source snapshot discovery ignores custom `-Z` compression settings.
  Files: `src/zxfer_get_zfs_list.sh` (`build_source_snapshot_list_cmd()`), `src/zxfer_zfs_send_receive.sh` (`wrap_command_with_ssh()`).
  Impact: remote snapshot discovery and remote send/receive compression still depend on whatever `zstd` the remote login shell resolves from its default `PATH`. In addition, the `-O ... -j ... -z` source-listing path hard-codes `zstd -9` / `zstd -d` instead of reusing the sanitized `-Z` command, so operators can get a different compression path for listing than for the replication stream.
  Recommended fix: resolve remote compression helpers through the same secure-PATH mechanism used for `zfs`, `cat`, and GNU `parallel`, and route the source-listing compression path through the same validated command settings used by the main send/receive pipeline.

- Remote-origin `-j` validation is still asymmetric for GNU `parallel`.
  Files: `src/zxfer_get_zfs_list.sh` (`ensure_parallel_available_for_source_jobs()`, `build_source_snapshot_list_cmd()`), `tests/test_zxfer_get_zfs_list.sh`, `tests/run_integration_zxfer.sh`.
  Impact: the remote-origin `-O ... -j ...` snapshot-listing pipeline executes GNU `parallel` only on the origin host, but zxfer still fails early if the local host lacks GNU `parallel`. At the same time, the remote probe only resolves a binary named `parallel` and never confirms that it is actually GNU `parallel`, unlike the local validation path. That means some valid remote-origin runs are rejected unnecessarily, while some invalid remote-origin setups can pass dependency checks and fail later with a non-GNU implementation.
  Recommended fix: validate local GNU `parallel` only when the generated pipeline actually uses it, and apply the same GNU-versus-non-GNU version check to the resolved remote origin binary.

- `ZXFER_COMPRESSION` is referenced by current diagnostics and tests, but the runtime does not actually support it.
  Files: `src/zxfer_globals.sh` (`refresh_compression_commands()`, `read_command_line_switches()`), `CHANGELOG.txt`, `tests/test_zxfer_common.sh`, `tests/test_zxfer_globals.sh`, `tests/run_integration_zxfer.sh`.
  Impact: current releases only honor `-Z`, yet the empty-command usage error and historical changelog entries still imply that `ZXFER_COMPRESSION` can supply the compression pipeline. Wrappers or operators that try to configure compression through the advertised environment variable silently fall back to the default `zstd -3` / `zstd -d` behavior.
  Recommended fix: either implement a real `ZXFER_COMPRESSION` configuration path (with matching decompression handling and docs) or remove the variable name from user-facing diagnostics, tests, and changelog history that describe current behavior.

- Common-snapshot detection still trusts snapshot names without validating snapshot identity.
  Files: `src/zxfer_inspect_delete_snap.sh` (`get_last_common_snapshot()`, `get_dest_snapshots_to_delete_per_dataset()`, `inspect_delete_snap()`), `src/zxfer_zfs_mode.sh` (`copy_snapshots()`, `reconcile_live_destination_snapshot_state()`, `rollback_destination_to_last_common_snapshot()`).
  Impact: zxfer treats `source@name` and `dest@name` as the same snapshot solely because the `@name` strings match. If source and destination each created unrelated snapshots with the same name, zxfer can misclassify them as a valid common base, skip deletion of divergent destination snapshots that merely share names, roll the destination back to the wrong `@name`, and then attempt an incremental send from a snapshot lineage that the destination does not actually have. That usually degrades into replication failure, but it can also distort delete and rollback decisions before the send fails.
  Recommended fix: validate common snapshots by GUID or other lineage-safe identity data from both sides before using them as delete, rollback, or incremental-send anchors, and keep name-only matching as a fallback only when identity cannot be queried safely.

- Remote backup metadata handling remains asymmetric versus the local path.
  Files: `src/zxfer_globals.sh` (`ensure_remote_backup_dir()`, `read_remote_backup_file()`, `get_backup_properties()`, `write_backup_properties()`).
  Impact: a non-root remote `-k` backup can create secure metadata owned by the ssh user, but a later remote `-e` restore fails closed because `read_remote_backup_file()` rejects any owner other than UID 0. Remote restore also lacks the local `find`-based fallback under `ZXFER_BACKUP_DIR`, so a valid secure backup that local restore would discover can still be missed remotely. In addition, live remote backup writes still invoke target-side `cat` by bare name rather than through a resolved helper path.
  Recommended fix: align remote backup-file ownership validation with `backup_owner_uid_is_allowed()` semantics, port the ambiguity-checked `find` fallback to the remote path, and resolve the target-side write helper through the same secure-lookup model used elsewhere.

- Backup metadata write/read lookup is keyed to different filesystem trees.
  Files: `src/zxfer_globals.sh` (`get_backup_properties()`, `write_backup_properties()`, `get_backup_storage_dir()`).
  Impact: `-k` writes the secure backup file under a directory derived from the destination root mountpoint, while `-e` searches directories derived from the source filesystem ancestry. When source and destination mountpoints differ, the primary secure lookup cannot find the file that the matching backup run wrote. Local restore usually survives only because it falls back to a broad filename search under `ZXFER_BACKUP_DIR`; remote restore has no such fallback and therefore misses otherwise valid backups more often.
  Recommended fix: key both backup writes and restores from the same stable identifier set (for example source dataset plus destination root), then remove the dependence on whole-tree `find` scans to recover from layout mismatches.

- Backup metadata filenames still use only the last source path component.
  Files: `src/zxfer_globals.sh` (`write_backup_properties()`, `get_backup_properties()`).
  Impact: backup files are named `.zxfer_backup_info.<tail>`, where `<tail>` is only `${initial_source##*/}`. Distinct replication roots such as `tank/a/src` and `tank/b/src` therefore map to the same filename whenever they share the same secure backup directory, so later runs can overwrite earlier metadata and make `-e` restores pick up the wrong replication set.
  Recommended fix: include a collision-resistant identifier derived from the full source dataset (or source-plus-destination pair) in the backup filename instead of using only the tail component.

- Backup-metadata manpages still describe the old mountpoint-based path and filename conventions.
  Files: `man/zxfer.8`, `man/zxfer.1m`, `src/zxfer_globals.sh` (`get_backup_storage_dir()`, `write_backup_properties()`, `get_backup_properties()`), `tests/test_zxfer_globals.sh`.
  Impact: the current manpages still say restore searches `ZXFER_BACKUP_DIR/mountpoint/.zxfer_backup_info.<tail>` and that `-k` writes `.zxfer_backup_info.<pool_name>` under `ZXFER_BACKUP_DIR/mountpoint/`. Current code instead writes `.zxfer_backup_info.<tail>` under the hardened `get_backup_storage_dir()` layout, which sanitizes mountpoints and uses special `root`, `legacy`, `none`, and `detached` directory forms. Operators trying to inspect, migrate, or pre-provision backup metadata by following the manpages can therefore look in the wrong place or expect the wrong filename format.
  Recommended fix: update the manpages to describe the actual hardened backup layout and naming rules, including the sanitized mountpoint handling and the detached/legacy/none special cases.

- Backup file headers still record ambiguous source/destination metadata.
  Files: `src/zxfer_globals.sh` (`write_backup_properties()`), `tests/run_integration_zxfer.sh`.
  Impact: the backup header stores `#initial_source:${initial_source##*/}` and `#destination:$g_destination`, while the body rows use the full source dataset and the per-row actual destination (`$g_actual_dest`). For nested datasets, trailing-slash restores, or multi-dataset recursive backups, that means the header can describe only the source tail and the destination root rather than the exact dataset pair represented by each row. Operators inspecting a backup file can therefore misidentify what it belongs to, and the current restore path cannot use the header to disambiguate conflicting rows even if it wanted to.
  Recommended fix: write full, unambiguous source/destination identifiers into the header (or drop the misleading header fields entirely) and keep the file-level metadata aligned with the row format that restore mode actually consumes.

- Restore mode still does not validate the backup file header or version marker.
  Files: `src/zxfer_globals.sh` (`get_backup_properties()`, `write_backup_properties()`), `src/zxfer_transfer_properties.sh` (`collect_source_props()`), `tests/test_zxfer_globals.sh`, `tests/run_integration_zxfer.sh`.
  Impact: current writes add a `#zxfer property backup file` header plus version metadata, but restore never checks for that marker or enforces any format version before consuming rows. A secure file with no header, or even one with an unrelated first line, is still accepted as long as it contains a source-matching row. That means future format changes cannot be rejected cleanly, and manually created or stale files can be mistaken for valid zxfer metadata without any explicit compatibility check.
  Recommended fix: require a valid zxfer backup-file header before restore, parse and validate the stored format/version fields, and fail closed on unknown or missing metadata formats instead of silently treating the file as current.

- Backup metadata reads still follow nested symlink path components.
  Files: `src/zxfer_globals.sh` (`read_local_backup_file()`, `read_remote_backup_file()`), `src/zxfer_common.sh` (`zxfer_find_symlink_path_component()`).
  Impact: local backup reads only reject the final metadata path when it is itself a symlink, and the remote secure-cat probe does the same with `[ -h "$path" ]`. If a parent directory inside `ZXFER_BACKUP_DIR` or a raw mountpoint fallback path is a symlink, restore still follows it and accepts the target file as long as its owner and mode checks pass. That leaves a restore-time trust gap even if backup-directory creation was previously hardened or the metadata file itself is not a symlink.
  Recommended fix: reject backup metadata paths whose parent components are symlinks before reading them, reusing the whole-path symlink detection approach already used for `ZXFER_ERROR_LOG`, and apply the same policy on remote reads.

- Restore mode still ignores the recorded destination dataset in backup metadata rows.
  Files: `src/zxfer_globals.sh` (`backup_metadata_matches_source()`, `get_backup_properties()`), `src/zxfer_transfer_properties.sh` (`collect_source_props()`).
  Impact: `get_backup_properties()` accepts any candidate backup file that contains the requested source dataset, without checking the recorded destination column or the backup header destination. Later, `collect_source_props()` restores properties by grepping only on the source dataset and stripping the first two comma-separated fields from every match. If the chosen metadata contains the same source dataset for a different destination, zxfer silently restores the wrong property set; if it contains multiple same-source rows, zxfer concatenates multiple property lists instead of selecting one exact match.
  Recommended fix: require an exact source-plus-destination match (and ideally one unique row) when selecting backup files and extracting restored property lists, using `g_actual_dest` and the stored destination header as additional validation.

- Secure backup path derivation is still not collision-resistant, and the raw mountpoint fallback can escape `ZXFER_BACKUP_DIR`.
  Files: `src/zxfer_globals.sh` (`sanitize_backup_component()`, `sanitize_dataset_relpath()`, `get_backup_storage_dir()`, `get_backup_properties()`).
  Impact: secure backup directories are derived by normalizing mountpoint components with `tr -c 'A-Za-z0-9._-' '_'`, so distinct custom mountpoints such as `/mnt/foo+bar`, `/mnt/foo?bar`, and `/mnt/foo bar` all collapse to the same secure path. In addition, restore-mode fallback still concatenates the literal mountpoint under `ZXFER_BACKUP_DIR`, so mountpoints containing `..` segments can probe paths outside the configured backup root. That means backup metadata naming is neither collision-free nor fully root-confined for unusual mountpoint layouts.
  Recommended fix: switch the secure backup layout to a reversible collision-resistant encoding and canonicalize or reject raw mountpoint fallback paths that would leave `ZXFER_BACKUP_DIR`.

- Backup-directory preparation still follows nested symlink path components.
  Files: `src/zxfer_globals.sh` (`ensure_local_backup_dir()`, `ensure_remote_backup_dir()`), `src/zxfer_common.sh` (`zxfer_find_symlink_path_component()`).
  Impact: local and remote backup-directory setup only rejects the final requested directory when it is itself a symlink. If any parent component of `ZXFER_BACKUP_DIR` or a derived secure backup subdirectory is a symlink, `mkdir -p` follows it and zxfer happily creates the “secure” metadata directory through that redirect. This leaves a gap between the current implementation and the hardening claims around keeping backup metadata outside attacker-controlled symlink paths.
  Recommended fix: reject backup paths with any symlinked component before `mkdir -p` or `chmod`, using the same whole-path check already implemented for `ZXFER_ERROR_LOG`, and apply equivalent validation on the remote helper path as well.

- `-U` unsupported-property detection still conflates property presence with property support.
  Files: `src/zxfer_zfs_mode.sh` (`calculate_unsupported_properties()`), `src/zxfer_transfer_properties.sh` (`remove_unsupported_properties()`, `strip_unsupported_properties()`).
  Impact: zxfer derives the “supported property” sets from `zfs get -Ho property all` on the source and destination pool roots, then strips any transferred property whose name only appears on the source side. That can falsely drop user properties or other dataset-specific properties when they are valid on the destination but simply not present on the destination pool root at probe time.
  Recommended fix: detect unsupported properties from actual destination capability failures (or a stable capability query) instead of using pool-root property presence as a proxy for support.

- Recursive `-o` overrides no longer preserve the documented “root set, children inherit” behavior.
  Files: `src/zxfer_transfer_properties.sh` (`derive_override_lists()`, `diff_properties()`, `apply_property_changes()`), `man/zxfer.8`, `man/zxfer.1m`.
  Impact: the manpages still say that recursive `-o compression=...` only sets the root dataset and lets descendants inherit from it. In current code, `derive_override_lists()` marks the override as `override` for every dataset, and `diff_properties()` then converts any matching child property whose current source is not `local` into a local `zfs set`. That means children which already inherit the desired parent value are still rewritten as local overrides, diverging from the documented interface and potentially applying type-specific overrides more broadly than intended.
  Recommended fix: preserve inherited override intent on descendant datasets when the parent already provides the requested effective value, and add regression coverage for recursive `-o` source-versus-inheritance behavior.

- Property transfer and backup serialization still cannot safely represent property values containing raw `,`, `=`, or `;`.
  Files: `src/zxfer_transfer_properties.sh` (`get_normalized_dataset_properties()`, `derive_override_lists()`, `diff_properties()`, `apply_property_changes()`), `src/zxfer_globals.sh` (`write_backup_properties()`, `get_backup_properties()`).
  Impact: zxfer serializes property state with ad hoc comma/equals/semicolon delimiters and reparses it with `cut`, `tr`, and `IFS=,` loops. Properties whose values legitimately contain those characters, including some share options and arbitrary user properties, can therefore be truncated, split into fake fields, or restored incorrectly.
  Recommended fix: replace the delimiter-packed property format with a tab/newline-safe representation (or add consistent escaping/unescaping) before extending property replication further.

- `-k` backup metadata can accumulate duplicate records across `-Y` iterations.
  Files: `src/zxfer_transfer_properties.sh` (`transfer_properties()`, `collect_source_props()`), `zxfer`.
  Impact: `transfer_properties()` appends raw source property rows to the global backup buffer every time a dataset gets a property pass, while the launcher writes the backup file only once after `run_zfs_mode_loop()` completes. On a multi-iteration `-Y` run, the same dataset can therefore be recorded multiple times in one backup file. A later `-e` restore greps by source dataset and can misparse those duplicate rows as a single comma-delimited property list.
  Recommended fix: store backup metadata keyed by source dataset (or deduplicate before write) instead of appending repeated rows into one flat string.

- `-k` backup metadata is only flushed after the entire replication loop exits successfully.
  Files: `src/zxfer_transfer_properties.sh` (`transfer_properties()`), `zxfer`, `src/zxfer_globals.sh` (`write_backup_properties()`).
  Impact: zxfer collects backup rows in `g_backup_file_contents` during property reconciliation, but the launcher does not call `write_backup_properties()` until after `run_zfs_mode_loop()` returns. If a later dataset or send/receive step fails, the process exits through `trap_exit` first and no backup file is written at all, even for datasets whose property state was already collected. That makes `-k` less durable than operators may assume on partially successful runs.
  Recommended fix: write backup metadata incrementally (or stage it atomically per completed dataset / iteration) instead of deferring the only on-disk write until final process success.

- Chained `-k` backups still cannot preserve original-source properties across intermediate override hops.
  Files: `src/zxfer_transfer_properties.sh` (`collect_source_props()`, `transfer_properties()`), `man/zxfer.8`, `man/zxfer.1m`.
  Impact: when backing up a backup to another location, the final `-k` metadata is always built from the intermediate source dataset’s live properties (`m_source_pvs_raw`). zxfer has no path to carry forward the earlier `.zxfer_backup_info` contents as the provenance source for the next hop. As a result, overrides or local property changes applied on the intermediate backup become the recorded “original” properties for later `-e` restores from the final backup. The manpages already warn that this is not yet supported, and the current runtime still behaves that way.
  Recommended fix: add a mode that propagates prior backup metadata forward when the source was itself produced from a zxfer property backup, or explicitly encode original-source provenance separately from the intermediate dataset’s live state.

- Dry-run `-n` still executes snapshot-discovery and remote-preflight commands.
  Files: `zxfer` (`prepare_remote_host_connections`, `init_variables`), `src/zxfer_globals.sh` (`prepare_remote_host_connections()`, `init_variables()`), `src/zxfer_get_zfs_list.sh` (`write_source_snapshot_list_to_file()`, `write_destination_snapshot_list_to_files()`), `src/zxfer_common.sh` (`execute_background_cmd()`), `man/zxfer.8`, `man/zxfer.1m`.
  Impact: the `-n` documentation says zxfer prints commands without executing them, but remote runs still prepare ssh control sockets and probe remote OS/helper paths during launcher preflight, and snapshot discovery still executes its background listing commands even when dry-run is enabled. In practice, `-n` can therefore open ssh sessions, prompt for authentication, and run read-only local or remote `zfs list` pipelines before printing the main replication commands. This is less severe than the `-n -k` backup-directory mutation below, but it still violates the documented print-only contract.
  Recommended fix: gate remote preflight and snapshot-discovery execution behind `g_option_n_dryrun`, or split dry-run into separate “render only” and “validate against live state” modes so operators can choose whether any commands should run.

- Dry-run `-n -s` and `-n -m` still perform source-side snapshot and migration actions.
  Files: `src/zxfer_zfs_mode.sh` (`run_zfs_mode()`, `maybe_capture_preflight_snapshot()`, `prepare_migration_services()`, `stopsvcs()`), `man/zxfer.8`, `man/zxfer.1m`.
  Impact: `-n -s` still calls `newsnap "$initial_source"` and refreshes the cached dataset state, so a dry run can create a real snapshot on the source. `-n -m` is worse: it still disables any SMF services requested with `-c`, unmounts every recursive source dataset, takes the migration snapshot, and refreshes the dataset lists before the main replication phase. That means a supposedly non-executing dry run can still alter live source state in exactly the areas operators use `-s` and `-m` to protect most carefully.
  Recommended fix: skip `maybe_capture_preflight_snapshot()` and `prepare_migration_services()` entirely in dry-run mode, or convert them to render-only previews that describe the intended snapshot/service/unmount actions without executing them.

- Dry-run `-n -k` still mutates local and remote backup directories during preflight.
  Files: `src/zxfer_zfs_mode.sh` (`check_backup_storage_dir_if_needed()`), `src/zxfer_globals.sh` (`ensure_local_backup_dir()`, `ensure_remote_backup_dir()`), `man/zxfer.8`, `man/zxfer.1m`.
  Impact: the `-n` documentation says zxfer prints commands without executing them, but `run_zfs_mode()` still calls `check_backup_storage_dir_if_needed()` before any dry-run gating. As a result, `-n -k` can create or chmod `ZXFER_BACKUP_DIR` locally, and `-n -k -T host` still runs the remote backup-directory preparation helper over ssh. This is a real side effect on runs that operators may expect to be read-only.
  Recommended fix: skip backup-directory creation/hardening in dry-run mode (or report the intended backup-directory actions without executing them) while still preserving fail-closed validation for explicitly requested live runs.

- OpenZFS-on-macOS property integration remains less deterministic than FreeBSD/Linux for some inherited child-dataset properties.
  File: `tests/run_integration_zxfer.sh` (`property_creation_with_zvol_test()`, `property_override_and_ignore_test()`).
  Impact: Darwin integration tests currently skip the strict child `atime=off` assertions because that behavior has not been made stable enough to use as a portable end-to-end gate. This is currently treated as a platform-specific certification gap rather than a proven production data-loss bug.
  Recommended fix: investigate the exact property-source/value differences on Darwin after receive and property reconciliation, then either normalize zxfer's post-receive behavior or narrow the documented expectations for that platform.

- FreeBSD still reports some created or updated dataset properties with unexpected source classifications (`default` versus `local`).
  Files: `src/zxfer_globals.sh` (`g_fbsd_readonly_properties`), `src/zxfer_transfer_properties.sh` (`transfer_properties()`).
  Impact: properties such as `quota`, `reservation`, `canmount`, `refquota`, and `refreservation` can land with a different property source than operators might expect after `zfs create` / `zfs set`. This does not currently affect inheritable-property reconciliation, but it remains a platform-specific behavior difference worth documenting.
  Recommended fix: keep FreeBSD-specific assertions narrow in tests and investigate whether post-create reconciliation can normalize the reported property source without introducing cross-platform regressions.

## Security Review

- The codebase still relies on `eval` for several command-construction paths.
  Files: `src/zxfer_common.sh` (`get_os()`, `execute_command()`, `execute_background_cmd()`, `exists_destination()`), `src/zxfer_get_zfs_list.sh` (source/destination list execution).
  Risk: current quoting helpers and tokenization rules significantly reduce direct injection risk, but this remains an architecture-sensitive area. A future change that passes partially quoted or newly user-controlled text into these helpers could reopen command-injection bugs.
  Recommended direction: prefer argv-based execution helpers wherever practical, and treat any new `eval` call or new `l_cmd="..."` string as a security-sensitive review point.

- Remote trust-boundary hardening is incomplete outside the main `zfs`/`cat`/`parallel` paths.
  Files: `src/zxfer_common.sh` (`get_os()`), `src/zxfer_get_zfs_list.sh` (remote `zstd` pipeline), `src/zxfer_zfs_send_receive.sh` (remote compression pipeline), `src/zxfer_globals.sh` (`ensure_remote_backup_dir()`, `read_remote_backup_file()`, `write_backup_properties()`).
  Risk: zxfer's secure PATH model is strong for the core `zfs`, `cat`, and GNU `parallel` helpers, but a compromised remote login shell can still influence `uname`, `zstd`, and several backup-metadata helper commands (`stat`, `ls`, `grep`, `awk`, `id`, `mkdir`, `chmod`, and the target-side `cat` used for remote writes) in these remaining paths.
  Recommended direction: eliminate default-shell PATH dependence for remote helper execution, or document the residual trust assumptions more explicitly in the user docs.

## Testing Limitations

- `tests/run_integration_zxfer.sh` is file-backed and considerably safer than earlier versions, but it is not fully sandboxed.
  File: `tests/run_integration_zxfer.sh`.
  Impact: the harness avoids raw devices and should only create/destroy its own file-backed pools, but it still performs real kernel ZFS operations, mount activity, and dataset changes on the host.
  Current state: this is partially mitigated by the dedicated GitHub Actions integration workflow, which now runs the file-backed harness on `ubuntu-24.04` with ZFS installed at job runtime.
  Operational guidance: use a disposable VM, throwaway host, or CI runner for zero-risk validation; do not describe the harness as fully sandboxed.
