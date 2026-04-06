# KNOWN ISSUES

This file tracks open issues that still matter for current releases. Issues are
ordered by remediation priority: exploitable security flaws and destructive
correctness bugs first, then reliability and interface drift, then lower-risk
documentation and portability gaps.

Generic architecture notes are intentionally omitted unless they currently
describe a concrete failure mode or exploit path.

## Critical

- [Security] Resolved local helper paths from `ZXFER_SECURE_PATH` are only validated as “absolute,” not shell-safe.
  Files: `src/zxfer_globals.sh` (`zxfer_find_required_tool()`, `zxfer_assign_required_tool()`), `src/zxfer_get_zfs_list.sh` (`build_source_snapshot_list_cmd()`, `write_source_snapshot_list_to_file()`), `src/zxfer_common.sh` (`execute_background_cmd()`, `execute_command()`, `exists_destination()`), `src/zxfer_zfs_send_receive.sh` (`get_send_command()`, `get_receive_command()`).
  Risk: local helper discovery has the same validation gap as the remote path, but via `ZXFER_SECURE_PATH`. `zxfer_find_required_tool()` accepts any `command -v` result that begins with `/`, even if the pathname contains shell-significant characters such as command substitutions. Several later code paths then splice `g_cmd_zfs` or other resolved helpers into `eval`-built command strings. In a harness, setting `ZXFER_SECURE_PATH` to a directory literally named `.../bin$(touch <marker>)` made `build_source_snapshot_list_cmd()` render `/tmp/.../bin$(touch <marker>)/zfs list ...`, and `execute_background_cmd()` created the marker file when it `eval`ed that command string.
  Recommended fix: apply the same shell-safety validation to locally resolved helper paths, and stop building `eval` command strings from resolved helper-path variables. Treat resolved helpers as argv elements, not shell fragments.

- [Security] Local GNU `parallel` resolution still bypasses helper-path validation entirely.
  Files: `src/zxfer_globals.sh` (`init_globals()`), `src/zxfer_get_zfs_list.sh` (`ensure_parallel_available_for_source_jobs()`, `build_source_snapshot_list_cmd()`), `src/zxfer_common.sh` (`execute_background_cmd()`).
  Risk: unlike `zfs`, `ssh`, and `cat`, the local `parallel` binary is not resolved through `zxfer_find_required_tool()`. `init_globals()` assigns `g_cmd_parallel` directly from `command -v parallel`, and `ensure_parallel_available_for_source_jobs()` only checks `"$g_cmd_parallel" --version` before `build_source_snapshot_list_cmd()` interpolates that path into an `eval`-executed pipeline. In a harness, setting `g_cmd_parallel` to a fake GNU parallel under a directory named `.../bin$(touch <marker>)` made the built command include that command substitution, and `execute_background_cmd()` created the marker file when it evaluated the listing pipeline.
  Recommended fix: resolve local GNU `parallel` through the same absolute-path and shell-safety validation used for other required helpers, and stop embedding the resulting path into `eval` strings.

- [Security] SSH control-socket paths are interpolated into `eval` command strings without shell quoting.
  Files: `src/zxfer_globals.sh` (`setup_ssh_control_socket()`, `get_ssh_cmd_for_host()`, `refresh_remote_zfs_commands()`), `src/zxfer_get_zfs_list.sh` (`build_source_snapshot_list_cmd()`, `write_source_snapshot_list_to_file()`), `src/zxfer_common.sh` (`execute_background_cmd()`, `execute_command()`).
  Risk: `get_ssh_cmd_for_host()` renders control-socket reuse as a plain string like `ssh -S <socket>`, and `refresh_remote_zfs_commands()` splices that string directly into `g_LZFS` / `g_RZFS`. Later snapshot-list and helper paths feed those composite command strings through `eval`. In a harness, setting `g_ssh_origin_control_socket` to a path containing `$(touch <marker>)` made `build_source_snapshot_list_cmd()` render that command substitution in the `ssh -S ...` prefix, and `execute_background_cmd()` created the marker file when it evaluated the command. Because `setup_ssh_control_socket()` sources these socket paths from `mktemp` under `${TMPDIR:-/tmp}`, a shell-significant `TMPDIR` segment or similarly malformed socket path can become a local command-execution vector.
  Recommended fix: treat control-socket paths as argv data rather than string fragments, and stop composing `ssh -S <socket>` into `eval`-executed command text. At minimum, quote or validate socket paths for shell safety before they ever reach `g_LZFS` / `g_RZFS`.

- [Data integrity] Destination-existence probes still treat operational failures as “dataset absent”.
  Files: `src/zxfer_common.sh` (`exists_destination()`), `src/zxfer_zfs_mode.sh` (`copy_snapshots()`, `reconcile_live_destination_snapshot_state()`, `rollback_destination_to_last_common_snapshot()`), `src/zxfer_transfer_properties.sh` (`ensure_destination_exists()`).
  Impact: `exists_destination()` currently runs `zfs list` and returns `0` for every non-zero outcome, so ssh failures, permission denials, wrapper misconfiguration, and transient remote errors are all collapsed into the same “dataset does not exist” signal. That false absence then drives higher-level control flow: `copy_snapshots()` can enter the missing-destination seed path, `rollback_destination_to_last_common_snapshot()` can silently skip a needed rollback because the destination is believed absent, and `ensure_destination_exists()` can enable parent-creation mode (`-p`) because a parent probe failed rather than because the parent is truly missing.
  Recommended fix: make destination-existence checks distinguish “not found” from other failures and fail closed on probe errors instead of treating every error as proof of absence.

- [Data integrity] Existing-destination live snapshot rechecks still treat probe failures as “empty destination”.
  Files: `src/zxfer_zfs_mode.sh` (`reconcile_live_destination_snapshot_state()`, `copy_snapshots()`).
  Impact: when `g_dest_has_snapshots=0` but the destination dataset already exists, zxfer tries a live `zfs list -t snapshot` recheck before it decides whether to seed the dataset. That recheck currently uses `run_destination_zfs_cmd ... || :`, so ssh failures, permission errors, or other listing errors collapse into an empty result. `copy_snapshots()` then falls through the “exists but has no snapshots” branch and temporarily enables `-F` for a full seed receive. On a destination that actually does have snapshots, a transient listing failure can therefore be misinterpreted as proof of emptiness and drive a destructive reseed path.
  Recommended fix: fail closed when the live destination snapshot probe errors, or require a positive success result that proves the dataset has zero snapshots before entering the forced seed branch.

- [Data integrity] Common-snapshot detection still trusts snapshot names without validating snapshot identity.
  Files: `src/zxfer_inspect_delete_snap.sh` (`get_last_common_snapshot()`, `get_dest_snapshots_to_delete_per_dataset()`, `inspect_delete_snap()`), `src/zxfer_zfs_mode.sh` (`copy_snapshots()`, `reconcile_live_destination_snapshot_state()`, `rollback_destination_to_last_common_snapshot()`).
  Impact: zxfer treats `source@name` and `dest@name` as the same snapshot solely because the `@name` strings match. If source and destination each created unrelated snapshots with the same name, zxfer can misclassify them as a valid common base, skip deletion of divergent destination snapshots that merely share names, roll the destination back to the wrong `@name`, and then attempt an incremental send from a snapshot lineage that the destination does not actually have.
  Recommended fix: validate common snapshots by GUID or other lineage-safe identity data from both sides before using them as delete, rollback, or incremental-send anchors.

- [Data integrity] Must-create property lookup failures still disable the creation-time mismatch guard.
  Files: `src/zxfer_transfer_properties.sh` (`ensure_required_properties_present()`, `transfer_properties()`, `diff_properties()`).
  Impact: zxfer relies on `ensure_required_properties_present()` to append creation-time properties such as `casesensitivity`, `normalization`, `jailed`, and `utf8only` when `zfs get all` omits them. That helper currently treats a failed explicit `zfs get -Hpo property,value,source <prop> <dataset>` probe as “property absent” and silently continues. If one of those explicit lookups fails on the source or destination, `diff_properties()` can run without the must-create property and skip the guard that should fail when source and destination differ in a creation-time setting.
  Recommended fix: fail closed when a required creation-time property is missing from `zfs get all` and its explicit probe fails, rather than silently treating the property as unavailable.

- [Data integrity] Source type/volsize probe failures can still create a filesystem where a zvol was required.
  Files: `src/zxfer_transfer_properties.sh` (`transfer_properties()`, `ensure_destination_exists()`, `run_zfs_create_with_properties()`).
  Impact: `transfer_properties()` reads the source dataset type and volsize with `run_source_zfs_cmd get -Hpo value type|volsize ...`, but it never checks whether those probes succeeded. For a missing destination, `ensure_destination_exists()` passes the resulting values straight into `run_zfs_create_with_properties()`, which only adds `-V <volsize>` when `l_source_dstype=volume` and `l_source_volsize` is non-empty. A failed volsize probe can therefore fall through to a filesystem create instead of a zvol create.
  Recommended fix: fail closed when the source dataset type or required zvol size probe fails, and require a validated non-empty `volsize` before continuing with destination creation for `type=volume`.

- [Safety] Dry-run `-n -s` and `-n -m` still perform source-side snapshot and migration actions.
  Files: `src/zxfer_zfs_mode.sh` (`run_zfs_mode()`, `maybe_capture_preflight_snapshot()`, `prepare_migration_services()`, `stopsvcs()`), `man/zxfer.8`, `man/zxfer.1m`.
  Impact: `-n -s` still calls `newsnap "$initial_source"` and refreshes the cached dataset state, so a dry run can create a real snapshot on the source. `-n -m` is worse: it still disables any SMF services requested with `-c`, unmounts recursive source datasets, takes the migration snapshot, and refreshes dataset lists before the main replication phase. That means a supposedly non-executing dry run can still alter live source state in exactly the areas operators use `-s` and `-m` to protect most carefully.
  Recommended fix: skip `maybe_capture_preflight_snapshot()` and `prepare_migration_services()` entirely in dry-run mode, or convert them to render-only previews.

## High

- [Reliability] Remote `zfs send` / `zfs receive` still shell out to the local `zfs` path when `-O` or `-T` is used.
  Files: `src/zxfer_zfs_send_receive.sh` (`get_send_command()`, `get_receive_command()`, `zfs_send_receive()`), `src/zxfer_globals.sh` (`init_variables()`).
  Impact: zxfer now probes and stores remote `zfs` paths in `g_origin_cmd_zfs` / `g_target_cmd_zfs`, but the main replication stream still builds send/receive commands with `g_cmd_zfs`. Mixed-platform or mixed-layout hosts can therefore pass dependency validation and snapshot discovery, then fail during the actual replication stream because the remote host does not have `zfs` at the local absolute path.
  Recommended fix: build send/receive commands from the already resolved remote `zfs` paths, or from argv-based source/target command helpers, instead of hard-coding `g_cmd_zfs` into the stream command string.

- [Security/Reliability] Remote compression still depends on bare remote `zstd`, and source snapshot discovery ignores custom `-Z` compression settings.
  Files: `src/zxfer_get_zfs_list.sh` (`build_source_snapshot_list_cmd()`), `src/zxfer_zfs_send_receive.sh` (`wrap_command_with_ssh()`).
  Impact: remote snapshot discovery and remote send/receive compression still depend on whatever `zstd` the remote login shell resolves from its default `PATH`. In addition, the `-O ... -j ... -z` source-listing path hard-codes `zstd -9` / `zstd -d` instead of reusing the sanitized `-Z` command, so operators can get a different compression path for listing than for the replication stream.
  Recommended fix: resolve remote compression helpers through the same secure-PATH mechanism used for `zfs`, `cat`, and GNU `parallel`, and route the source-listing compression path through the same validated command settings used by the main send/receive pipeline.

- [Reliability] `-U` destination capability probe failures still collapse into “supports nothing”.
  Files: `src/zxfer_zfs_mode.sh` (`calculate_unsupported_properties()`), `src/zxfer_transfer_properties.sh` (`remove_unsupported_properties()`, `strip_unsupported_properties()`).
  Impact: `calculate_unsupported_properties()` does not check whether `run_destination_zfs_cmd get -Ho property all "$l_dest_pool_name"` succeeded. When that probe fails, the destination property list becomes empty, every source property is marked unsupported, and the later `strip_unsupported_properties()` pass can silently drop the entire property-transfer set. A transient remote or probe failure under `-U` therefore degrades into “replicate no properties” instead of aborting.
  Recommended fix: fail closed when the destination capability query fails, or only derive `unsupported_properties` from a positively validated capability result.

- [Data integrity] Property transfer and backup serialization still cannot safely represent property values containing raw `,`, `=`, or `;`.
  Files: `src/zxfer_transfer_properties.sh` (`get_normalized_dataset_properties()`, `derive_override_lists()`, `diff_properties()`, `apply_property_changes()`), `src/zxfer_globals.sh` (`write_backup_properties()`, `get_backup_properties()`).
  Impact: zxfer serializes property state with ad hoc comma/equals/semicolon delimiters and reparses it with `cut`, `tr`, and `IFS=,` loops. Properties whose values legitimately contain those characters, including some share options and arbitrary user properties, can therefore be truncated, split into fake fields, or restored incorrectly.
  Recommended fix: replace the delimiter-packed property format with a tab/newline-safe representation, or add consistent escaping and unescaping.

- [Reliability] Remote backup metadata handling remains asymmetric versus the local path.
  Files: `src/zxfer_globals.sh` (`ensure_remote_backup_dir()`, `read_remote_backup_file()`, `get_backup_properties()`, `write_backup_properties()`).
  Impact: a non-root remote `-k` backup can create secure metadata owned by the ssh user, but a later remote `-e` restore fails closed because `read_remote_backup_file()` rejects any owner other than UID 0. Remote restore also lacks the local `find`-based fallback under `ZXFER_BACKUP_DIR`, so a valid secure backup that local restore would discover can still be missed remotely. In addition, live remote backup writes still invoke target-side `cat` by bare name rather than through a resolved helper path.
  Recommended fix: align remote backup-file ownership validation with `backup_owner_uid_is_allowed()` semantics, port the ambiguity-checked `find` fallback to the remote path, and resolve the target-side write helper through the same secure-lookup model used elsewhere.

- [Reliability] Backup metadata write/read lookup is keyed to different filesystem trees.
  Files: `src/zxfer_globals.sh` (`get_backup_properties()`, `write_backup_properties()`, `get_backup_storage_dir()`).
  Impact: `-k` writes the secure backup file under a directory derived from the destination root mountpoint, while `-e` searches directories derived from the source filesystem ancestry. When source and destination mountpoints differ, the primary secure lookup cannot find the file that the matching backup run wrote. Local restore usually survives only because it falls back to a broad filename search under `ZXFER_BACKUP_DIR`; remote restore has no such fallback and therefore misses otherwise valid backups more often.
  Recommended fix: key both backup writes and restores from the same stable identifier set, then remove the dependence on whole-tree `find` scans to recover from layout mismatches.

- [Reliability] Backup metadata filenames still use only the last source path component.
  Files: `src/zxfer_globals.sh` (`write_backup_properties()`, `get_backup_properties()`).
  Impact: backup files are named `.zxfer_backup_info.<tail>`, where `<tail>` is only `${initial_source##*/}`. Distinct replication roots such as `tank/a/src` and `tank/b/src` therefore map to the same filename whenever they share the same secure backup directory, so later runs can overwrite earlier metadata and make `-e` restores pick up the wrong replication set.
  Recommended fix: include a collision-resistant identifier derived from the full source dataset, or source-plus-destination pair, in the backup filename instead of using only the tail component.

- [Data integrity] Restore mode still ignores the recorded destination dataset in backup metadata rows.
  Files: `src/zxfer_globals.sh` (`backup_metadata_matches_source()`, `get_backup_properties()`), `src/zxfer_transfer_properties.sh` (`collect_source_props()`).
  Impact: `get_backup_properties()` accepts any candidate backup file that contains the requested source dataset, without checking the recorded destination column or the backup header destination. Later, `collect_source_props()` restores properties by grepping only on the source dataset and stripping the first two comma-separated fields from every match. If the chosen metadata contains the same source dataset for a different destination, zxfer silently restores the wrong property set; if it contains multiple same-source rows, zxfer concatenates multiple property lists instead of selecting one exact match.
  Recommended fix: require an exact source-plus-destination match, and ideally one unique row, when selecting backup files and extracting restored property lists.

- [Security/Reliability] Secure backup path derivation is still not collision-resistant, and the raw mountpoint fallback can escape `ZXFER_BACKUP_DIR`.
  Files: `src/zxfer_globals.sh` (`sanitize_backup_component()`, `sanitize_dataset_relpath()`, `get_backup_storage_dir()`, `get_backup_properties()`).
  Impact: secure backup directories are derived by normalizing mountpoint components with `tr -c 'A-Za-z0-9._-' '_'`, so distinct custom mountpoints such as `/mnt/foo+bar`, `/mnt/foo?bar`, and `/mnt/foo bar` all collapse to the same secure path. In addition, restore-mode fallback still concatenates the literal mountpoint under `ZXFER_BACKUP_DIR`, so mountpoints containing `..` segments can probe paths outside the configured backup root.
  Recommended fix: switch the secure backup layout to a reversible collision-resistant encoding and canonicalize or reject raw mountpoint fallback paths that would leave `ZXFER_BACKUP_DIR`.

- [Security] Backup-directory preparation still follows nested symlink path components.
  Files: `src/zxfer_globals.sh` (`ensure_local_backup_dir()`, `ensure_remote_backup_dir()`), `src/zxfer_common.sh` (`zxfer_find_symlink_path_component()`).
  Impact: local and remote backup-directory setup only rejects the final requested directory when it is itself a symlink. If any parent component of `ZXFER_BACKUP_DIR` or a derived secure backup subdirectory is a symlink, `mkdir -p` follows it and zxfer happily creates the “secure” metadata directory through that redirect.
  Recommended fix: reject backup paths with any symlinked component before `mkdir -p` or `chmod`, using the same whole-path check already implemented for `ZXFER_ERROR_LOG`, and apply equivalent validation on the remote helper path as well.

- [Security] Backup metadata reads still follow nested symlink path components.
  Files: `src/zxfer_globals.sh` (`read_local_backup_file()`, `read_remote_backup_file()`), `src/zxfer_common.sh` (`zxfer_find_symlink_path_component()`).
  Impact: local backup reads only reject the final metadata path when it is itself a symlink, and the remote secure-cat probe does the same with `[ -h "$path" ]`. If a parent directory inside `ZXFER_BACKUP_DIR` or a raw mountpoint fallback path is a symlink, restore still follows it and accepts the target file as long as its owner and mode checks pass.
  Recommended fix: reject backup metadata paths whose parent components are symlinks before reading them, and apply the same policy on remote reads.

- [Availability] Migration service relaunch still abandons remaining services after the first enable failure.
  Files: `src/zxfer_zfs_mode.sh` (`relaunch()`), `src/zxfer_globals.sh` (`trap_exit()`).
  Impact: `relaunch()` clears `g_services_need_relaunch` before any `svcadm enable` succeeds, then aborts on the first enable failure. If multiple services were disabled for `-m -c ...`, a failure reenabling one service leaves the later services unattempted and still stopped. Because both `relaunch()` and `trap_exit()` clear the relaunch-needed flag before the restart sequence completes, the shutdown path also loses the signal that more services still need recovery.
  Recommended fix: keep restart-needed state asserted until all requested services have been re-enabled successfully, and continue attempting the remaining services while collecting failures.

- [Safety] Dry-run `-n -k` still mutates local and remote backup directories during preflight.
  Files: `src/zxfer_zfs_mode.sh` (`check_backup_storage_dir_if_needed()`), `src/zxfer_globals.sh` (`ensure_local_backup_dir()`, `ensure_remote_backup_dir()`), `man/zxfer.8`, `man/zxfer.1m`.
  Impact: the `-n` documentation says zxfer prints commands without executing them, but `run_zfs_mode()` still calls `check_backup_storage_dir_if_needed()` before any dry-run gating. As a result, `-n -k` can create or chmod `ZXFER_BACKUP_DIR` locally, and `-n -k -T host` still runs the remote backup-directory preparation helper over ssh.
  Recommended fix: skip backup-directory creation and hardening in dry-run mode, or report the intended backup-directory actions without executing them.

- [Security] Temporary-file and progress-FIFO roots still trust caller-supplied `TMPDIR` without validating the parent directory.
  Files: `src/zxfer_common.sh` (`get_temp_file()`), `src/zxfer_zfs_send_receive.sh` (`zxfer_progress_passthrough()`), `src/zxfer_globals.sh` (`trap_exit()` cleanup of `g_zxfer_temp_prefix` artifacts), `tests/test_zxfer_common.sh`.
  Risk: zxfer intentionally honors `TMPDIR` for snapshot-list caches, diff files, restore lookup scratch files, and the progress FIFO path. In an elevated or environment-preserving run, a hostile `TMPDIR` can therefore steer those artifacts into an attacker-controlled parent directory that zxfer never ownership-checks or scans for symlink components. The progress path is weaker still: it allocates a name with `mktemp`, immediately removes that file, and then recreates the same pathname with `mkfifo`, reopening a race window inside the caller-chosen temp root.
  Recommended fix: ignore `TMPDIR` in privileged mode or require it to resolve under a trusted non-symlink directory owned by root or the effective UID, and build the progress FIFO inside a private `mktemp -d` directory instead of deleting and recreating a pathname in place.

- [Security] Hardened pathname checks are still non-atomic for `ZXFER_ERROR_LOG` and backup metadata I/O.
  Files: `src/zxfer_common.sh` (`zxfer_append_failure_report_to_log()`, `zxfer_create_error_log_file()`), `src/zxfer_globals.sh` (`read_local_backup_file()`, `read_remote_backup_file()`, `write_backup_properties()`).
  Risk: these helpers validate a pathname first, then reopen that same pathname later with `cat`, `>`, or `>>`. `ZXFER_ERROR_LOG` is especially exposed because its parent directory only has to exist; it does not have to be owner-controlled. A competing local process can therefore swap in a different file after the owner/mode/symlink checks but before the append or create happens.
  Recommended fix: move these flows to atomic open/create primitives so validation and I/O operate on the same object, or create into a trusted temporary file and `rename` it into place only after validating the final destination path.

## Medium

- [Security/Reliability] `ZXFER_SECURE_PATH` is not authoritative for the live runtime `PATH`.
  Files: `src/zxfer_globals.sh` (`zxfer_compute_secure_path()`, `merge_path_allowlists()`, `zxfer_apply_secure_path()`), `tests/test_zxfer_globals.sh`, `man/zxfer.8`, `man/zxfer.1m`, `CHANGELOG.txt`.
  Risk: the documentation and changelog describe `ZXFER_SECURE_PATH` as the whitelist of trusted helper directories, but `zxfer_apply_secure_path()` always merges the built-in system directories back into `PATH` even when the operator explicitly overrides the allowlist. Dependency resolution does use the strict configured path, yet every later bare helper invocation that still relies on `PATH` can resolve from the built-in directories anyway.
  Recommended fix: when `ZXFER_SECURE_PATH` is set, keep the runtime `PATH` confined to that exact allowlist, or finish converting the remaining bare helper calls to absolute resolved paths so the runtime `PATH` no longer matters.

- [Security] `ZXFER_BACKUP_DIR` still accepts relative paths, so “secure” metadata can silently follow the current working directory.
  Files: `src/zxfer_globals.sh` (`zxfer_refresh_backup_storage_root()`, `get_backup_storage_dir()`, `ensure_local_backup_dir()`, `ensure_remote_backup_dir()`), `man/zxfer.8`, `man/zxfer.1m`.
  Risk: unlike `ZXFER_ERROR_LOG`, the backup-root override is not required to be absolute. If the environment sets `ZXFER_BACKUP_DIR=relative-backups`, zxfer will happily create and use `./relative-backups/...` locally and will pass the same relative path to remote helpers under `-O` / `-T`, making the effective storage root depend on the caller's or remote shell's current directory.
  Recommended fix: require `ZXFER_BACKUP_DIR` to be an absolute canonical path, locally and remotely, or reject relative overrides outright.

- [Security] Remote SSH host-authentication policy is still inherited entirely from ambient ssh configuration.
  Files: `src/zxfer_globals.sh` (`zxfer_assign_required_tool()`, `setup_ssh_control_socket()`, `get_ssh_cmd_for_host()`), `src/zxfer_common.sh` (`invoke_ssh_command_for_host()`, `invoke_ssh_shell_command_for_host()`), `src/zxfer_zfs_send_receive.sh` (`wrap_command_with_ssh()`).
  Risk: zxfer resolves `ssh` to an absolute path, but it never adds its own `StrictHostKeyChecking`, `UserKnownHostsFile`, `BatchMode`, or similar transport-safety options. That means remote replication, backup-metadata probes, and helper-path discovery all inherit whatever host-key and authentication policy the local user's ssh config happens to allow.
  Recommended fix: add an explicit safe default ssh option set for zxfer-managed connections, or a dedicated opt-out, and document clearly when the tool is relying on ambient ssh trust policy.

- [Security] Structured failure reports can disclose operator-supplied secrets from custom command hooks and wrappers.
  Files: `zxfer` (`g_zxfer_original_invocation` capture), `src/zxfer_common.sh` (`zxfer_render_failure_report()`, `zxfer_record_last_command_string()`, `zxfer_record_last_command_argv()`, `zxfer_append_failure_report_to_log()`), `man/zxfer.8`, `man/zxfer.1m`.
  Risk: every non-zero exit emits the original shell-quoted invocation plus the last attempted command to `stderr`, and `ZXFER_ERROR_LOG` mirrors the same block verbatim. Any sensitive material operators place in custom hook strings such as `-D`, `-Z`, wrapper-style host specs, or other ad hoc shell fragments is therefore written back out on failure.
  Recommended fix: add a mode that suppresses or redacts `invocation` and `last_command` in failure reports, and document clearly that secret-bearing arguments should not be passed on the zxfer command line while the current reporting format remains verbatim.

- [Security] Structured failure reports still pass through raw terminal control characters.
  Files: `zxfer` (`zxfer_escape_report_value_early()`, `zxfer_quote_token_for_report_early()`), `src/zxfer_common.sh` (`zxfer_escape_report_value()`, `zxfer_quote_token_for_report()`, `zxfer_render_failure_report()`, `zxfer_append_failure_report_to_log()`), `tests/test_zxfer_common.sh`.
  Risk: the failure-report escapers currently normalize backslashes, tabs, carriage returns, and embedded newlines, but they leave other non-printable bytes such as ANSI escape sequences untouched. In terminal-driven or pager-driven workflows, that allows log or terminal injection such as color spoofing, cursor movement, or other control-sequence side effects inside the structured report.
  Recommended fix: escape or strip all non-printable control bytes before rendering report fields, and add regression coverage that asserts `invocation:` and `last_command:` never contain raw control characters.

- [Reliability] Parallel send/receive failure detection is still serialized by launch order.
  Files: `src/zxfer_zfs_send_receive.sh` (`wait_for_zfs_send_jobs()`, `zfs_send_receive()`).
  Impact: zxfer records background send/receive shell PIDs in launch order and then waits on them sequentially. If an earlier transfer is still running while a later transfer has already failed, zxfer does not notice that failure or attempt to kill the remaining jobs until the earlier PID exits. A fast failure in one dataset can therefore sit undetected behind a long-running earlier job.
  Recommended fix: switch to prompt failure detection for background jobs, for example `wait -n` where available, or per-job status files and polling with process-group teardown.

- [Reliability] `-g` grandfather-protection probe failures still misclassify snapshots as ancient.
  Files: `src/zxfer_inspect_delete_snap.sh` (`grandfather_test()`, `delete_snaps()`).
  Impact: `grandfather_test()` does not check whether `run_destination_zfs_cmd get -H -o value -p creation "$l_destination_snapshot"` succeeded or returned a numeric epoch. When that probe fails, the empty value is fed into shell arithmetic as zero, so zxfer reports the snapshot as roughly 1970-era and blocks deletion with a misleading grandfather-protection error.
  Recommended fix: validate the creation-time probe before doing age arithmetic, and fail with an explicit destination metadata lookup error instead of treating unknown timestamps as protected ancient snapshots.

- [Reliability] Backup metadata mountpoint probe failures still collapse into the detached layout.
  Files: `src/zxfer_globals.sh` (`get_backup_properties()`, `write_backup_properties()`).
  Impact: both backup restore and backup write paths call `run_source_zfs_cmd` or `run_destination_zfs_cmd get -H -o value mountpoint ...` without checking whether the mountpoint query actually succeeded. If that probe fails, the empty result is treated the same as a real blank or detached mountpoint. Restore can then read a matching file from `ZXFER_BACKUP_DIR/detached/...` even though the source mountpoint lookup failed, and `-k` writes can target `.../detached/...` unexpectedly instead of aborting.
  Recommended fix: fail closed when the mountpoint lookup errors, and only use the detached layout when a successful probe explicitly reports a detached or blank mountpoint.

- [Compatibility] `-U` unsupported-property detection still conflates property presence with property support.
  Files: `src/zxfer_zfs_mode.sh` (`calculate_unsupported_properties()`), `src/zxfer_transfer_properties.sh` (`remove_unsupported_properties()`, `strip_unsupported_properties()`).
  Impact: zxfer derives the “supported property” sets from `zfs get -Ho property all` on the source and destination pool roots, then strips any transferred property whose name only appears on the source side. That can falsely drop user properties or other dataset-specific properties when they are valid on the destination but simply not present on the destination pool root at probe time.
  Recommended fix: detect unsupported properties from actual destination capability failures, or a stable capability query, instead of using pool-root property presence as a proxy for support.

- [Interface] Recursive `-o` overrides no longer preserve the documented “root set, children inherit” behavior.
  Files: `src/zxfer_transfer_properties.sh` (`derive_override_lists()`, `diff_properties()`, `apply_property_changes()`), `man/zxfer.8`, `man/zxfer.1m`.
  Impact: the manpages still say that recursive `-o compression=...` only sets the root dataset and lets descendants inherit from it. In current code, `derive_override_lists()` marks the override as `override` for every dataset, and `diff_properties()` then converts any matching child property whose current source is not `local` into a local `zfs set`. Children which already inherit the desired parent value are therefore still rewritten as local overrides.
  Recommended fix: preserve inherited override intent on descendant datasets when the parent already provides the requested effective value, and add regression coverage for recursive `-o` inheritance behavior.

- [Reliability] `-k` backup metadata can accumulate duplicate records across `-Y` iterations.
  Files: `src/zxfer_transfer_properties.sh` (`transfer_properties()`, `collect_source_props()`), `zxfer`.
  Impact: `transfer_properties()` appends raw source property rows to the global backup buffer every time a dataset gets a property pass, while the launcher writes the backup file only once after `run_zfs_mode_loop()` completes. On a multi-iteration `-Y` run, the same dataset can therefore be recorded multiple times in one backup file. A later `-e` restore greps by source dataset and can misparse those duplicate rows as a single comma-delimited property list.
  Recommended fix: store backup metadata keyed by source dataset, or deduplicate before write, instead of appending repeated rows into one flat string.

- [Durability] `-k` backup metadata is only flushed after the entire replication loop exits successfully.
  Files: `src/zxfer_transfer_properties.sh` (`transfer_properties()`), `zxfer`, `src/zxfer_globals.sh` (`write_backup_properties()`).
  Impact: zxfer collects backup rows in `g_backup_file_contents` during property reconciliation, but the launcher does not call `write_backup_properties()` until after `run_zfs_mode_loop()` returns. If a later dataset or send/receive step fails, the process exits through `trap_exit` first and no backup file is written at all, even for datasets whose property state was already collected.
  Recommended fix: write backup metadata incrementally, or stage it atomically per completed dataset or iteration, instead of deferring the only on-disk write until final process success.

- [Interface] Dry-run `-n` still executes snapshot-discovery and remote-preflight commands.
  Files: `zxfer` (`prepare_remote_host_connections`, `init_variables`), `src/zxfer_globals.sh` (`prepare_remote_host_connections()`, `init_variables()`), `src/zxfer_get_zfs_list.sh` (`write_source_snapshot_list_to_file()`, `write_destination_snapshot_list_to_files()`), `src/zxfer_common.sh` (`execute_background_cmd()`), `man/zxfer.8`, `man/zxfer.1m`.
  Impact: the `-n` documentation says zxfer prints commands without executing them, but remote runs still prepare ssh control sockets and probe remote OS and helper paths during launcher preflight, and snapshot discovery still executes its background listing commands even when dry-run is enabled.
  Recommended fix: gate remote preflight and snapshot-discovery execution behind `g_option_n_dryrun`, or split dry-run into separate render-only and validate-against-live-state modes.

- [Interface] Dry-run `-n -D` still executes live send-estimate probes.
  Files: `src/zxfer_zfs_send_receive.sh` (`calculate_size_estimate()`, `handle_progress_bar_option()`, `zfs_send_receive()`), `man/zxfer.8`, `man/zxfer.1m`.
  Impact: when a progress command is configured with `-D`, zxfer still calls `run_source_zfs_cmd send -nPv ...` to estimate stream size before it renders the dry-run pipeline. That means `-n -D` can still hit local or remote source hosts, prompt for ssh authentication, and fail on read-side send or permission errors even though the actual replication command is never executed.
  Recommended fix: skip progress-size probing in dry-run mode, or render the progress command with an unknown-size placeholder unless the operator explicitly opts into live validation.

- [Compatibility] Restore mode still does not validate the backup file header or version marker.
  Files: `src/zxfer_globals.sh` (`get_backup_properties()`, `write_backup_properties()`), `src/zxfer_transfer_properties.sh` (`collect_source_props()`), `tests/test_zxfer_globals.sh`, `tests/run_integration_zxfer.sh`.
  Impact: current writes add a `#zxfer property backup file` header plus version metadata, but restore never checks for that marker or enforces any format version before consuming rows. A secure file with no header, or even one with an unrelated first line, is still accepted as long as it contains a source-matching row. That means future format changes cannot be rejected cleanly, and manually created or stale files can be mistaken for valid zxfer metadata without any explicit compatibility check.
  Recommended fix: require a valid zxfer backup-file header before restore, parse and validate the stored format or version fields, and fail closed on unknown or missing metadata formats.

## Low

- [Security/Reliability] Remote OS detection still goes through `eval` and the remote login shell's default `PATH`.
  Files: `src/zxfer_common.sh` (`get_os()`), `src/zxfer_globals.sh` (`init_variables()`).
  Impact: remote platform detection can execute the remote account's `uname` instead of a securely resolved binary, and shell behavior differences remain in play earlier than the hardened remote `zfs` and `cat` lookup paths. This mostly affects feature gating and platform-specific property handling, not the main replication stream.
  Recommended fix: replace `get_os()` with the same argv-based ssh helper path used by `run_source_zfs_cmd()` and `run_destination_zfs_cmd()`, or resolve `uname` remotely through the secure PATH before use.

- [Compatibility] Remote-origin `-j` validation is still asymmetric for GNU `parallel`.
  Files: `src/zxfer_get_zfs_list.sh` (`ensure_parallel_available_for_source_jobs()`, `build_source_snapshot_list_cmd()`), `tests/test_zxfer_get_zfs_list.sh`, `tests/run_integration_zxfer.sh`.
  Impact: the remote-origin `-O ... -j ...` snapshot-listing pipeline executes GNU `parallel` only on the origin host, but zxfer still fails early if the local host lacks GNU `parallel`. At the same time, the remote probe only resolves a binary named `parallel` and never confirms that it is actually GNU `parallel`, unlike the local validation path. That means some valid remote-origin runs are rejected unnecessarily, while some invalid remote-origin setups can pass dependency checks and fail later with a non-GNU implementation.
  Recommended fix: validate local GNU `parallel` only when the generated pipeline actually uses it, and apply the same GNU-versus-non-GNU version check to the resolved remote origin binary.

- [Compatibility] SSH control-socket setup and teardown still misuse wrapper-style host specs.
  Files: `src/zxfer_globals.sh` (`setup_ssh_control_socket()`, `close_origin_ssh_control_socket()`, `close_target_ssh_control_socket()`, `prepare_remote_host_connections()`), `README.md`, `man/zxfer.8`, `man/zxfer.1m`, `tests/test_zxfer_globals.sh`.
  Impact: wrapper-style remote specs such as `host pfexec` and `host doas` are documented for the actual remote command path, but the control-socket management helpers currently append those wrapper tokens directly to `ssh -M -S ... -fN` and `ssh -S ... -O exit`. Those are transport-control operations, not remote command executions, so wrapped remote runs can fail during preflight or fail to close the multiplexed connection even though the later remote command path itself is valid.
  Recommended fix: strip wrapper tokens out of control-socket create and close commands and pass only the ssh destination host there, while preserving the wrapper tokens for later remote-command invocations.

- [Interface] `ZXFER_COMPRESSION` is referenced by current diagnostics and tests, but the runtime does not actually support it.
  Files: `src/zxfer_globals.sh` (`refresh_compression_commands()`, `read_command_line_switches()`), `CHANGELOG.txt`, `tests/test_zxfer_common.sh`, `tests/test_zxfer_globals.sh`, `tests/run_integration_zxfer.sh`.
  Impact: current releases only honor `-Z`, yet the empty-command usage error and historical changelog entries still imply that `ZXFER_COMPRESSION` can supply the compression pipeline. Wrappers or operators that try to configure compression through the advertised environment variable silently fall back to the default `zstd -3` / `zstd -d` behavior.
  Recommended fix: either implement a real `ZXFER_COMPRESSION` configuration path, with matching decompression handling and docs, or remove the variable name from current diagnostics, tests, and changelog text.

- [Documentation] Backup-metadata manpages still describe the old mountpoint-based path and filename conventions.
  Files: `man/zxfer.8`, `man/zxfer.1m`, `src/zxfer_globals.sh` (`get_backup_storage_dir()`, `write_backup_properties()`, `get_backup_properties()`), `tests/test_zxfer_globals.sh`.
  Impact: the current manpages still say restore searches `ZXFER_BACKUP_DIR/mountpoint/.zxfer_backup_info.<tail>` and that `-k` writes `.zxfer_backup_info.<pool_name>` under `ZXFER_BACKUP_DIR/mountpoint/`. Current code instead writes `.zxfer_backup_info.<tail>` under the hardened `get_backup_storage_dir()` layout, which sanitizes mountpoints and uses special `root`, `legacy`, `none`, and `detached` directory forms.
  Recommended fix: update the manpages to describe the actual hardened backup layout and naming rules.

- [Observability] Backup file headers still record ambiguous source/destination metadata.
  Files: `src/zxfer_globals.sh` (`write_backup_properties()`), `tests/run_integration_zxfer.sh`.
  Impact: the backup header stores `#initial_source:${initial_source##*/}` and `#destination:$g_destination`, while the body rows use the full source dataset and the per-row actual destination. For nested datasets, trailing-slash restores, or multi-dataset recursive backups, that means the header can describe only the source tail and the destination root rather than the exact dataset pair represented by each row. Operators inspecting a backup file can therefore misidentify what it belongs to.
  Recommended fix: write full, unambiguous source and destination identifiers into the header, or drop the misleading header fields entirely.

- [Limitation] Chained `-k` backups still cannot preserve original-source properties across intermediate override hops.
  Files: `src/zxfer_transfer_properties.sh` (`collect_source_props()`, `transfer_properties()`), `man/zxfer.8`, `man/zxfer.1m`.
  Impact: when backing up a backup to another location, the final `-k` metadata is always built from the intermediate source dataset’s live properties. zxfer has no path to carry forward the earlier `.zxfer_backup_info` contents as the provenance source for the next hop. As a result, overrides or local property changes applied on the intermediate backup become the recorded “original” properties for later `-e` restores from the final backup. The manpages already warn that this is not yet supported.
  Recommended fix: add a mode that propagates prior backup metadata forward when the source was itself produced from a zxfer property backup, or explicitly encode original-source provenance separately from the intermediate dataset’s live state.

## Testing Limitations

- `tests/run_integration_zxfer.sh` is file-backed and considerably safer than earlier versions, but it is not fully sandboxed.
  File: `tests/run_integration_zxfer.sh`.
  Impact: the harness avoids raw devices and should only create and destroy its own file-backed pools, but it still performs real kernel ZFS operations, mount activity, and dataset changes on the host.
  Current state: this is partially mitigated by the dedicated GitHub Actions integration workflow, which runs the file-backed harness on `ubuntu-24.04` with ZFS installed at job runtime.
  Operational guidance: use a disposable VM, throwaway host, or CI runner for zero-risk validation; do not describe the harness as fully sandboxed.

- OpenZFS-on-macOS property integration remains less deterministic than FreeBSD and Linux for some inherited child-dataset properties.
  File: `tests/run_integration_zxfer.sh` (`property_creation_with_zvol_test()`, `property_override_and_ignore_test()`).
  Impact: Darwin integration tests currently skip the strict child `atime=off` assertions because that behavior has not been made stable enough to use as a portable end-to-end gate. This is currently a platform-specific certification gap rather than a proven production data-loss bug.
  Recommended follow-up: investigate the exact property-source and value differences on Darwin after receive and property reconciliation, then either normalize zxfer's post-receive behavior or narrow the documented expectations for that platform.
