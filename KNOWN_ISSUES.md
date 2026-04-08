# KNOWN ISSUES

This file tracks open issues that still matter for current releases. Issues are
ordered by remediation priority: exploitable security flaws and destructive
correctness bugs first, then reliability and interface drift, then lower-risk
documentation and portability gaps.

Generic architecture notes are intentionally omitted unless they currently
describe a concrete failure mode or exploit path.

## High

- [Interface] The comma-delimited `-o` override syntax still cannot carry raw commas inside one property value.
  Files: `src/zxfer_globals.sh` (`read_command_line_switches()`), `src/zxfer_transfer_properties.sh` (`derive_override_lists()`).
  Impact: live source properties and backup metadata now escape raw `,`, `=`, and `;` safely, and `-o` override values now preserve raw `=` and `;` after the first separator. However, the CLI still tokenizes `-o` on raw commas before value escaping, so an operator-supplied override such as `-o user:note=value,with,commas` is still split into fake assignments.
  Recommended fix: add an explicit comma-escaping parser for `-o`, or introduce a repeatable override flag that accepts one `property=value` assignment per option instance.

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

- [Security/Reliability] Remote backup-directory and metadata guard helpers still trust the ambient remote `PATH` for auxiliary tools.
  Files: `src/zxfer_globals.sh` (`ensure_remote_backup_dir()`, `read_remote_backup_file()`).
  Risk: the secure-PATH model now resolves remote `zfs`, `cat`, GNU `parallel`, `find`, and compression helpers, but the remote backup-dir and backup-metadata guard scripts still invoke `stat`, `ls`, `id`, `grep`, `awk`, `mkdir`, and `chmod` by bare name. A hostile or misconfigured remote `PATH` can therefore change ownership checks, permission checks, or backup-directory preparation behavior even when zxfer's resolved helper paths are otherwise locked down.
  Recommended fix: run these remote helper scripts under the same validated remote `PATH`, or resolve their auxiliary commands explicitly before composing the remote shell command.

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

- [Reliability] Backup metadata mountpoint probe failures still distort restore compatibility fallback lookup.
  Files: `src/zxfer_globals.sh` (`get_backup_properties()`).
  Impact: secure backup writes now use the source-dataset-relative tree and no longer depend on mountpoints, but restore still probes the source mountpoint before trying older compatibility paths. When that probe fails, the empty result is treated the same as a real blank or detached mountpoint, so restore can inspect `ZXFER_BACKUP_DIR/detached/...` compatibility locations even though the source mountpoint lookup itself failed.
  Recommended fix: fail closed when the mountpoint lookup errors, and only use detached or mountpoint-derived compatibility paths when a successful probe explicitly reports that layout.

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
  Impact: `transfer_properties()` appends raw source property rows to the global backup buffer every time a dataset gets a property pass, while the launcher writes the backup file only once after `run_zfs_mode_loop()` completes. On a multi-iteration `-Y` run, the same dataset can therefore be recorded multiple times in one backup file. Restore now fails closed on ambiguous exact source/destination duplicates instead of concatenating them, but that still turns the generated backup metadata into an unusable ambiguous file.
  Recommended fix: store backup metadata keyed by source dataset, or deduplicate before write, instead of appending repeated rows into one flat string.

- [Durability] `-k` backup metadata is only flushed after the entire replication loop exits successfully.
  Files: `src/zxfer_transfer_properties.sh` (`transfer_properties()`), `zxfer`, `src/zxfer_globals.sh` (`write_backup_properties()`).
  Impact: zxfer collects backup rows in `g_backup_file_contents` during property reconciliation, but the launcher does not call `write_backup_properties()` until after `run_zfs_mode_loop()` returns. If a later dataset or send/receive step fails, the process exits through `trap_exit` first and no backup file is written at all, even for datasets whose property state was already collected.
  Recommended fix: write backup metadata incrementally, or stage it atomically per completed dataset or iteration, instead of deferring the only on-disk write until final process success.

- [Interface] Dry-run `-n` still executes snapshot-discovery and remote-preflight commands.
  Files: `zxfer` (`prepare_remote_host_connections`, `init_variables`), `src/zxfer_globals.sh` (`prepare_remote_host_connections()`, `init_variables()`), `src/zxfer_get_zfs_list.sh` (`write_source_snapshot_list_to_file()`, `write_destination_snapshot_list_to_files()`), `src/zxfer_common.sh` (`execute_background_cmd()`), `man/zxfer.8`, `man/zxfer.1m`.
  Impact: the `-n` documentation says zxfer prints commands without executing them, but remote runs still prepare ssh control sockets and probe remote OS and helper paths during launcher preflight, and snapshot discovery still executes its background listing commands even when dry-run is enabled.
  Recommended fix: gate remote preflight and snapshot-discovery execution behind `g_option_n_dryrun`, or split dry-run into separate render-only and validate-against-live-state modes.

- [Interface] Dry-run `-n -D` still executes live progress-size probes when the template uses `%%size%%`.
  Files: `src/zxfer_zfs_send_receive.sh` (`calculate_size_estimate()`, `handle_progress_bar_option()`, `zfs_send_receive()`), `man/zxfer.8`, `man/zxfer.1m`.
  Impact: when the configured progress template contains `%%size%%`, `handle_progress_bar_option()` still probes the live source before rendering a dry-run pipeline. Local single-job runs still use the exact `zfs send -nPv` estimate, while remote or `-j` runs now prefer cheaper `written@...` or `referenced` probes with exact fallback. Either way, `-n -D` can still contact local or remote source hosts, prompt for ssh authentication, and fail on source-side metadata or send-estimate errors even though the replication command itself is never executed. Templates that omit `%%size%%` no longer probe.
  Recommended fix: skip progress-size probing entirely in dry-run mode, or render the progress command with an unknown-size placeholder unless the operator explicitly opts into live validation.

- [Compatibility] Restore mode still does not validate the backup file header or version marker.
  Files: `src/zxfer_globals.sh` (`get_backup_properties()`, `write_backup_properties()`), `src/zxfer_transfer_properties.sh` (`collect_source_props()`), `tests/test_zxfer_globals.sh`, `tests/run_integration_zxfer.sh`.
  Impact: current writes add a `#zxfer property backup file` header plus version metadata, but restore never checks for that marker or enforces any format version before consuming rows. A secure file with no header, or even one with an unrelated first line, is still accepted as long as it contains one exact source/destination row. That means future format changes cannot be rejected cleanly, and manually created or stale files can be mistaken for valid zxfer metadata without any explicit compatibility check.
  Recommended fix: require a valid zxfer backup-file header before restore, parse and validate the stored format or version fields, and fail closed on unknown or missing metadata formats.

## Low

- [Compatibility] Remote adaptive discovery still trusts the resolved origin-host `parallel` binary without confirming it is GNU `parallel`.
  Files: `src/zxfer_get_zfs_list.sh` (`ensure_parallel_available_for_source_jobs()`, `build_source_snapshot_list_cmd()`), `src/zxfer_globals.sh` (`resolve_remote_required_tool()`), `tests/test_zxfer_get_zfs_list.sh`, `tests/run_integration_zxfer.sh`.
  Impact: adaptive `-j` source discovery now defers GNU `parallel` validation until it actually selects the per-dataset branch, and remote-origin runs no longer require a local `parallel` binary when only the remote branch will execute it. However, once that remote branch is selected, zxfer still trusts the resolved remote `parallel` path by name only. A non-GNU `parallel` implementation on the origin host can therefore pass startup validation and then fail later with different argv or output behavior.
  Recommended fix: when the remote per-dataset discovery path is selected, run a one-time remote `--version` check against the resolved origin-host helper and require a GNU `parallel` signature before building the command pipeline.

- [Compatibility] SSH control-socket setup and teardown still misuse wrapper-style host specs.
  Files: `src/zxfer_globals.sh` (`setup_ssh_control_socket()`, `close_origin_ssh_control_socket()`, `close_target_ssh_control_socket()`, `prepare_remote_host_connections()`), `README.md`, `man/zxfer.8`, `man/zxfer.1m`, `tests/test_zxfer_globals.sh`.
  Impact: wrapper-style remote specs such as `host pfexec` and `host doas` are documented for the actual remote command path, but the control-socket management helpers currently append those wrapper tokens directly to `ssh -M -S ... -fN` and `ssh -S ... -O exit`. Those are transport-control operations, not remote command executions, so wrapped remote runs can fail during preflight or fail to close the multiplexed connection even though the later remote command path itself is valid.
  Recommended fix: strip wrapper tokens out of control-socket create and close commands and pass only the ssh destination host there, while preserving the wrapper tokens for later remote-command invocations.

- [Interface] `ZXFER_COMPRESSION` is referenced by current diagnostics and tests, but the runtime does not actually support it.
  Files: `src/zxfer_globals.sh` (`refresh_compression_commands()`, `read_command_line_switches()`), `CHANGELOG.txt`, `tests/test_zxfer_common.sh`, `tests/test_zxfer_globals.sh`, `tests/run_integration_zxfer.sh`.
  Impact: current releases only honor `-Z`, yet the empty-command usage error and historical changelog entries still imply that `ZXFER_COMPRESSION` can supply the compression pipeline. Wrappers or operators that try to configure compression through the advertised environment variable silently fall back to the default `zstd -3` / `zstd -d` behavior.
  Recommended fix: either implement a real `ZXFER_COMPRESSION` configuration path, with matching decompression handling and docs, or remove the variable name from current diagnostics, tests, and changelog text.

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
