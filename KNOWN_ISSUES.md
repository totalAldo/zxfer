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

## Medium

- [Interface/Reliability] Host-spec and custom helper tokenization still ignores shell quoting and backslash escaping.
  Files: `src/zxfer_exec.sh` (`zxfer_split_tokens_on_whitespace()`, `zxfer_split_host_spec_tokens()`, `zxfer_split_cli_tokens()`), `src/zxfer_dependencies.sh` (`zxfer_resolve_local_cli_command_safe()`, `zxfer_requote_cli_command_with_resolved_head()`), `src/zxfer_cli.sh` (`zxfer_refresh_compression_commands()`).
  Impact: public string inputs such as `-O` / `-T` wrapper specs and `-Z` custom compression commands still flow through a whitespace-only tokenizer that ignores shell quoting and backslash escapes and intentionally injects token breaks around `;`, `|`, and `&`. Legitimate single arguments like `sudo -u "ZFS Admin"` or helper paths under directories with spaces therefore get silently split into the wrong argv, leading to failed dependency resolution, broken remote wrapper execution, or misreported helper errors instead of a clear validation failure.
  Recommended fix: either implement a safe POSIX shell-word parser for these public string interfaces, or fail closed by explicitly rejecting quoted/backslash-escaped inputs that cannot be represented losslessly instead of silently re-tokenizing them.

- [Safety/Reliability] Trap-exit cleanup still tracks bare background PIDs and can kill reused unrelated processes.
  Files: `src/zxfer_runtime.sh` (`zxfer_register_cleanup_pid()`, `zxfer_unregister_cleanup_pid()`, `zxfer_kill_registered_cleanup_pids()`, `zxfer_trap_exit()`), `src/zxfer_exec.sh` (`zxfer_execute_background_cmd()`), `src/zxfer_snapshot_discovery.sh` (`zxfer_execute_source_snapshot_list_in_background()`), `src/zxfer_send_receive.sh` (`zxfer_register_send_job()`, `zxfer_zfs_send_receive()`), `src/zxfer_snapshot_reconcile.sh` (`zxfer_build_snapshots_to_delete_list()`).
  Impact: zxfer still records only numeric PIDs for background helpers and, on trap exit, blindly sends `kill` to every registered PID. If a helper exits before it is unregistered and the PID is reused before zxfer aborts, trap cleanup can signal an unrelated local process instead of a zxfer-owned child. The same stale-PID path can also leave the real background helper uncollected while zxfer believes cleanup has run.
  Recommended fix: store stronger ownership metadata such as process start time or process-group identity, verify that a registered PID is still the expected zxfer-owned child before killing it, or move background cleanup to dedicated process groups instead of bare PID tracking.

- [Safety/Reliability] Abort cleanup still targets wrapper-shell PIDs instead of full background process groups.
  Files: `src/zxfer_exec.sh` (`zxfer_execute_background_cmd()`), `src/zxfer_send_receive.sh` (`zxfer_run_background_pipeline()`, `zxfer_zfs_send_receive()`, `zxfer_terminate_remaining_send_jobs()`), `src/zxfer_runtime.sh` (`zxfer_kill_registered_cleanup_pids()`, `zxfer_trap_exit()`).
  Impact: background snapshot-list and send/receive work is still launched via shell wrappers that `eval` pipelines in a child shell, while cleanup only records and kills the wrapper PID. On abort, killing that shell does not guarantee the pipeline grandchildren are reaped, so zxfer can exit while a `zfs send`, `zfs receive`, or remote ssh pipeline keeps running in the background. That widens the blast radius of a failed run because the operator may think cleanup completed when data-path processes are still active.
  Recommended fix: run background work in dedicated process groups or otherwise capture the full job lineage, and have cleanup signal the process group or a verified child set instead of only the wrapper shell PID.

- [Reliability/Observability] Successful live remote capability probes still treat local cache-write failures as warning-only success.
  Files: `src/zxfer_remote_hosts.sh` (`zxfer_write_remote_capability_cache_file()`, `zxfer_ensure_remote_host_capabilities()`, `zxfer_preload_remote_host_capabilities()`).
  Impact: after a live capability fetch succeeds, `zxfer_ensure_remote_host_capabilities()` now warns when `zxfer_write_remote_capability_cache_file()` fails, but it still returns success and leaves the broken local cache path eligible for more best-effort writes on later probes. Repeated runs can therefore keep falling back to live remote probes and re-emitting the same warning instead of transitioning the local cache into an explicit degraded state for the rest of the run.
  Recommended fix: either make local capability-cache persistence part of the checked success contract, or mark the local cache unavailable after the first warning so repeated live-probe fallback is an explicit degraded mode instead of repeated best-effort cache writes.

- [Reliability] Corrupt ssh control-socket identity files still get treated as benign cache-key collisions.
  Files: `src/zxfer_remote_hosts.sh` (`zxfer_ensure_ssh_control_socket_entry_dir()`, `zxfer_read_ssh_control_socket_entry_identity_file()`).
  Impact: when an existing control-socket entry has an unreadable, invalid, or otherwise failing `id` file, `zxfer_ensure_ssh_control_socket_entry_dir()` still treats that the same as a legitimate identity mismatch and just increments the numeric suffix. Repeated runs can therefore accumulate orphaned cache directories and bypass reuse of an entry whose local identity state is actually corrupted, instead of surfacing the broken cache metadata.
  Recommended fix: distinguish identity-file read or validation failures from true identity mismatches, and either fail closed or repair/reap the broken entry instead of silently allocating a suffixed sibling directory.

- [Reliability/Observability] exit cleanup still ignores ssh control-socket close failures.
  Files: `src/zxfer_runtime.sh` (`zxfer_trap_exit()`), `src/zxfer_remote_hosts.sh` (`zxfer_close_all_ssh_control_sockets()`).
  Impact: `zxfer_trap_exit()` still calls `zxfer_close_all_ssh_control_sockets()` and ignores its nonzero status. If final control-socket teardown fails after an otherwise successful replication, zxfer can still exit with the original status and emit no structured failure for the leaked master/socket-cache state. Operators may only get a best-effort stderr message while the command still reports success.
  Recommended fix: if cleanup hits a control-socket close failure while the main exit status is still `0`, promote the final exit to a runtime failure or at least capture the close error in the structured failure report so teardown leaks are not reported as success.

- [Reliability] Remaining wrapper-builder and probe helpers still collapse exact helper failures to generic exit status `1`.
  Files: `src/zxfer_send_receive.sh` (`zxfer_wrap_command_with_ssh()`), `src/zxfer_snapshot_discovery.sh` (`zxfer_render_remote_source_snapshot_serial_list_cmd()`, `zxfer_build_source_snapshot_list_cmd()`, `zxfer_local_parallel_functional_probe_reports_gnu()`).
  Impact: these helpers still use unchecked command substitution or `... || return 1` wrappers around lower-level helpers such as `zxfer_build_ssh_shell_command_for_host()` and `zxfer_read_runtime_artifact_file()`. When those lower-level helpers fail, the caller still flattens the real nonzero status to generic `1` and can blur the difference between local validation, readback, and transport/setup failures.
  Recommended fix: convert the remaining substitutions to the same current-shell exact-status pattern used by the runtime-artifact migrations: run the helper in an explicit `if ...; then ... else l_status=$?; return "$l_status"; fi` branch and only publish the captured string on success.

## SUGGESTED SOLUTIONS

Most of the remaining entries cluster around a small number of recurring implementation patterns:

- weak ownership tracking for background work and abort cleanup
- helpers that either flatten exact failures to generic exit status `1` or
  treat partial cache persistence/cleanup failures as warning-only success
- public string interfaces that treat shell syntax as unstructured whitespace

The highest-leverage path is to replace those patterns centrally instead of
continuing to fix each call site independently.

- Introduce a background job supervisor that owns process groups and completion records.
  Scope: this is the architectural fix for the current trap-exit PID reuse problem, wrapper-shell-only cleanup, and the broader class of background send/receive and snapshot-listing cleanup issues.
  Direction: launch long-lived background work under a single helper that records process-group identity plus start metadata, writes structured completion state, and exposes one verified teardown path. Abort cleanup should signal a validated process group or owned child set, not a bare PID captured from a wrapper shell.

- Enforce one result/status contract for reusable helpers.
  Scope: this addresses the remaining wrappers that still return success after partial local cache persistence/cleanup failures or normalize precise lower-level failures to generic exit status `1`.
  Direction: codify that a helper either returns `0` and completes its documented side effects, or returns the original nonzero status and leaves callers in an explicit degraded state. The current remaining migrations are concentrated in wrapper-builder and probe helpers such as `zxfer_wrap_command_with_ssh()`, the remote/parallel snapshot-discovery builders, and the remote capability cache warm path.

- Replace shell-like free-form string parsing with structured argv handling or strict rejection.
  Scope: this is the cross-cutting fix for host-spec tokenization, custom helper parsing, and other public interfaces that currently mis-handle quotes, escapes, and spaces.
  Direction: where compatibility allows, introduce structured alternatives such as repeatable argv-style flags or explicit wrapper/helper configuration variables. For legacy string interfaces that must remain, use one centralized strict parser or reject inputs that cannot be represented losslessly instead of silently re-tokenizing them.

If these solutions are implemented in order, the remaining three should retire
most of the remaining medium-severity issues.
