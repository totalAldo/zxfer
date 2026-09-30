# Coding Style

## Goals

`zxfer` shell code should be:

- safe on production ZFS hosts
- portable across supported `/bin/sh` implementations
- easy to review, test, and extend
- explicit about side effects, error handling, and operator-visible behavior

The project priority order still applies:

1. safety
2. security
3. maintainability
4. performance

## Shell Baseline

- Target POSIX `/bin/sh`.
- Do not add Bash-isms such as `[[ ... ]]`, arrays, `function`, `local`,
  process substitution, here-strings, or `$'...'` strings.
- Do not assume GNU-only flags or output formats unless they are gated by a
  compatibility check.
- Prefer `$(...)` command substitution over legacy backticks.
- Use `case` for multi-branch string dispatch instead of long `if` ladders when
  it improves readability.
- Avoid subshells when state must persist in the current shell.
- List characters explicitly in `case` patterns and globs (`[!0123456789]`,
  not `[!0-9]`): bash 3.2, the macOS `/bin/sh`, matches a bracket range by
  locale collation, so under a UTF-8 locale `[0-9]` also matches characters
  such as U+2185.
- A `VAR=value` prefix on a shell-function call does not reach the commands
  the function runs on FreeBSD `sh` (unless `VAR` is already exported) or
  ksh93. To hand a variable to them, export it in a subshell:
  `( VAR=value; export VAR; some_function )`.

## File And Module Layout

- Keep `src/` flat and organized by stable responsibility boundaries.
- Extend an existing module before creating a new one.
- Let [../src/zxfer_modules.sh](../src/zxfer_modules.sh) remain the single
  source-order authority for the launcher and direct-sourcing tests.
- Major `src/` modules should start with a short `Module contract` comment
  block that summarizes:
  - `owns globals`
  - `reads globals`
  - `mutates caches`
  - `returns via stdout`
- Keep module-contract headers short and high-signal. They should describe
  ownership boundaries and data flow, not restate every helper signature.
- Avoid generic filenames such as `common`, `globals`, `utils`, or `lib`.
- Use purpose-based module names such as `zxfer_reporting.sh`,
  `zxfer_snapshot_discovery.sh`, or `zxfer_send_receive.sh`.
- Name files for the domain they own, not for how widely their helpers are
  reused across the tree.
- Reserve `*_state` for modules that own mutable caches or shared session
  state.
- Reserve `*_runtime` for temp resources and process cleanup.
- Give each `src/zxfer_NAME.sh` one test entry, `tests/test_zxfer_NAME.sh`,
  whose fragments live in `tests/suites/zxfer_NAME_TOPIC_tests.sh`; behavior
  an operator sees belongs in the `tests/test_contract_*.sh` suites.

## Naming

- Shared helper functions should use the `zxfer_` prefix.
- Keep function names descriptive and action-oriented:
  `zxfer_render_*`, `zxfer_validate_*`, `zxfer_ensure_*`,
  `zxfer_reset_*`, `zxfer_get_*`, `zxfer_write_*`.
- Global shell state should use the existing `g_` prefix only for mutable
  runtime or session state.
- Parsed option state should use `g_option_*` and should not be reused as
  general scratch state.
- Function-scoped temporaries should use `l_` prefixes consistently. POSIX
  shell functions do not have local variables, so mutable scratch names must
  also be function-specific along every direct current-shell call edge (for
  example, `l_prepare_remote_host_status`, not a reusable `l_status` in both
  caller and callee). This is a review convention; no tool enforces it.
- Immutable internal constants may use `ZXFER_*`.
- Only documented operator-facing `ZXFER_*` environment variables are public
  configuration inputs; uppercase alone does not imply user configurability.
- Avoid naming mutable globals like constants.
- Environment variables intended for operators remain uppercase `ZXFER_*`.
- New public flags, env vars, exit codes, stderr/stdout formats, and help text
  are API changes and must be treated as such.

## Formatting

- Use tabs for shell indentation to match the existing `src/` style.
- Keep one logical step per line.
- Prefer early returns and small helpers over deeply nested conditionals.
- Break long pipelines and compound conditions across lines at natural
  boundaries.
- Keep `case` branches visually compact and aligned.
- Use blank lines to separate stages of a function, not every command.

## Quoting And Argument Handling

- Quote expansions unless field splitting is both intentional and safe.
- Prefer `"${var:-}"` or `"${var+...}"` patterns when unset variables are
  possible.
- Do not rely on implicit glob expansion.
- When building commands, preserve argument boundaries rather than stitching
  together shell strings.
- Reuse the argv renderers in
  [../src/zxfer_quoting.sh](../src/zxfer_quoting.sh), the remote renderers
  and runners in
  [../src/zxfer_ssh_transport.sh](../src/zxfer_ssh_transport.sh), and the
  execution helpers in [../src/zxfer_exec.sh](../src/zxfer_exec.sh) instead of
  adding new ad hoc `eval` paths.
- The only remaining production `eval` is the single hardened pipeline
  execution site in `zxfer_execute_rendered_shell_command()`. The
  `callers eval` ratchet in `tests/budget_policy.tsv` caps production `eval`
  at that one site; adding one requires an explicit purpose review.
- Treat `-O` / `-T` host specs and remote wrapper tokens as structured command
  inputs, not as plain hostnames.
- Build substantial remote helper protocols as readable multiline POSIX `sh`
  programs with explicit command terminators and focused golden coverage.
  Hand them to the transport through `zxfer_build_remote_sh_c_command`, which
  keeps a short one-line command plain and sends a longer or multi-line
  program as quoted chunks that a fixed `sh` bootstrap reassembles, so a
  csh/tcsh login shell never sees a raw newline or an overlong word. Only the
  capability probe and the backup dry-run display join nonblank lines into
  one line, and only at that transport boundary. Do not make line joining a
  general renderer API or apply it before configuration bytes have passed
  control-character checks.

## Dependency And Path Handling

- Resolve required tools through
  [../src/zxfer_dependencies.sh](../src/zxfer_dependencies.sh).
- Preserve the secure-PATH model and do not bypass it with unvalidated bare
  `PATH` lookups in feature code.
- Keep remote helper resolution inside
  [../src/zxfer_remote_hosts.sh](../src/zxfer_remote_hosts.sh).
- Reject tab, carriage-return, and line-feed bytes in
  `ZXFER_SECURE_PATH`, `ZXFER_SECURE_PATH_APPEND`, and resolved helper paths
  before splitting, caching, exporting, or remote rendering. Apply the same
  exact byte-shape rule to `ZXFER_BACKUP_DIR` before deriving local or remote
  metadata paths.

## Result Channels And Status

- Prefer stdout for pure values when command substitution cannot hide a needed
  state change or lower-level status.
- Use an owner-prefixed `g_zxfer_*_result` channel only for a deliberate
  current-shell/hot-path handoff. The owning module is the sole writer; it
  clears the channel before work, publishes only a complete validated result,
  clears it on failure, and preserves the first meaningful non-zero status.
- A caller must capture status before reading a result channel. Keep
  cross-module readers deliberate and few; no tool inventories them.
- Do not accept a caller-provided variable name and assign through `eval` as a
  generic return mechanism. Add a narrow owner result, explicit publisher, or
  ordinary stdout/status contract instead.

## Errors, Logging, And Output

- Route operator-facing failures through the reporting helpers in
  [../src/zxfer_reporting.sh](../src/zxfer_reporting.sh).
- Reusable helpers that publish scratch or result globals must follow one
  result/status contract: return `0` only after publishing a valid result, and
  on failure clear their result state and return the original non-zero status
  observed from the failing lower-level helper or command. Use direct status
  `1` only for zxfer-owned validation failures where there is no lower-level
  status to preserve.
- Preserve structured stderr failure reporting, failure classes, and failure
  stages.
- Prefer existing output helpers such as `zxfer_echov`, `zxfer_echoV`, and `zxfer_throw_error*`
  instead of printing new ad hoc messages. Print a value that may hold
  untrusted bytes, such as a property value, through `zxfer_escape_report_value`
  or `zxfer_echoV_escaped`, and print variable text with `printf '%s\n'`, not
  `echo`, whose backslash expansion differs between shells.
- Keep stdout/stderr behavior stable unless a compatibility change is
  intentional, documented, and tested.
- Make verbose output useful for operators. Avoid noisy debug text that does
  not help explain state, commands, or failures.

## Temporary Files, Cleanup, And Side Effects

- Use the runtime temp helpers in [../src/zxfer_runtime.sh](../src/zxfer_runtime.sh)
  instead of hard-coding `/tmp` paths.
- For runtime-temp-root artifacts, prefer the current-shell helpers in
  [../src/zxfer_runtime.sh](../src/zxfer_runtime.sh):
  `zxfer_create_runtime_artifact_file`,
  `zxfer_create_private_temp_dir`,
  `zxfer_write_runtime_artifact_file`,
  `zxfer_read_runtime_artifact_file`,
  `zxfer_cleanup_runtime_artifact_path`.
  They allocate under the one per-run 0700 temp root that trap exit removes
  with a single `rm -rf`.
- `zxfer_create_runtime_artifact_file` creates each 0600 file in the current
  shell under `umask 077` and noclobber, then restores the run umask recorded
  when the temp root was created, so callers must not hold a temporary umask
  across it. Helpers that overwrite a file the allocator just created use
  `>|`. `zxfer_trap_exit` restores noclobber, noglob, `IFS`, and the run umask
  before any cleanup, because a signal can land inside the allocator or a
  `zxfer_split_begin`/`zxfer_split_end` pair.
- Do not add new ad hoc runtime-temp-root `mktemp` calls, hard-coded `/tmp`
  scratch paths, raw `: >"$file"` truncation, unchecked `cat "$file"`
  readbacks, or unguarded `while ... done <"$file"` loops for staged payloads,
  captures, or runtime-owned cache objects.
- When a helper needs staged file contents, read them through the runtime
  readback helper, capture its status immediately, and only publish result
  globals after the read succeeds. Parse staged payloads from the in-memory
  scratch result rather than reading the file repeatedly.
- Keep path-adjacent secure staging in the owning module when same-directory
  atomic rename is the security requirement. The runtime artifact layer is for
  artifacts owned by the validated runtime temp root and runtime-owned cache
  files, not for every atomic publish flow in the tree.
- Register short-lived background PIDs with `zxfer_register_cleanup_pid`
  (send/receive jobs use the job registry in
  [../src/zxfer_send_jobs.sh](../src/zxfer_send_jobs.sh)); files under the
  run root need no registration.
- Remove temporary files, FIFOs, queues, and cache directories on both success
  and failure paths unless they live under the per-run temp root, which the
  exit trap removes as a whole, or are intentionally preserved for debugging.
- When startup or iteration reset needs module-owned scratch state, call the
  module's public reset helper instead of duplicating its `g_*` inventory in
  the runtime layer.
- Keep source-time side effects minimal. Runtime setup should happen in the
  explicit init flow, not merely because a module was sourced.

## Comments

- Comment why a block exists, not what the shell syntax already says.
- Every top-level function in `src/` should have an immediately preceding
  comment block in this short structured form:

```sh
# Purpose: Allocate one scratch file under the run root, or stop the run.
# Usage: zxfer_get_temp_file; publishes g_zxfer_temp_file_result.
zxfer_get_temp_file() {
```

- Use `Purpose:` and `Usage:` on every function comment block.
- Apply this requirement to top-level shell functions defined directly in
  `src/` modules. Function literals embedded inside generated shell payload
  strings are not source-level API helpers; document the enclosing builder
  function instead.
- Add `Returns:` or `Side effects:` only when stdout contracts, exit-status
  meaning, global mutation, staging, or cleanup behavior would not be
  obvious from the function body and name alone.
- Keep the block immediately above the function it documents.
- Explain why the helper exists and where it fits in zxfer's flow, not just a
  paraphrase of the function name.
- Keep function comments concise and high-signal. Do not add per-argument
  inventories unless they prevent a real misuse or ambiguity.
- Normalize existing function comments into the structured form instead of
  stacking a second header above them.
- Preserve still-relevant inline comments, block comments, and historical notes
  when they explain compatibility behavior, safety rationale, platform quirks,
  security constraints, or past regressions that would be costly to rediscover.
- Remove or rewrite comments only when they are clearly stale, duplicated, or
  contradicted by the current code.
- Do not replace valuable historical context with a generic function docblock.
- Add short comments for non-obvious `awk`, `sed`, `comm`, `parallel`, ssh, or
  quoting logic.
- Do not add comments that restate simple assignments or obvious control flow.
- When compatibility behavior is subtle, mention the affected platform or shell
  family directly in the comment.
- When a function is updated, review its function comment and any still-relevant
  nearby comments in the same change. If the implementation contract changes,
  update the comment immediately instead of leaving drift behind.

## Tests

- Add or update focused shunit2 coverage when changing shell helpers or public
  behavior.
- Keep [../tests/test_helper.sh](../tests/test_helper.sh) limited to loading
  every module, the test lifecycle, and process capture. Domain fixtures such
  as backup renderers or environment-driven fake tools stay in focused
  `tests/helpers/*_fixtures.sh` files and must be sourced explicitly only by
  the suites that own those cases.
- Keep fixtures explicit and local to the suite unless they are broadly useful.
- Start an entry suite's `setUp` with `zxfer_test_reset_all_owner_state`
  (directly or through a domain fixture's setup helper), then override only
  suite-specific values; move domain-specific preparation into named fixture
  helpers.
- Pin behavior an operator can observe in a contract suite, and unit-test
  only what the black-box harness cannot reach (see "Where a new test
  goes" in [testing.md](./testing.md)). Do not stub collaborators to pin
  the order of a module's calls.
- Stub `src/` functions inside a subshell so the stub cannot leak into later
  cases.
- Capture a command's status (`l_run_status=$?`) on the line after it before
  asserting on it. In `assertEquals "... $(cat ERR)" 0 $?`, bash, ksh and zsh
  expand `$?` to the substitution's status, so the check always passes.
- Put `./` before a test operand that starts with `-` (`mkdir ./-dash-dir`):
  GNU and uutils utilities permute arguments, so they read `-dash-dir` as
  options even after another operand.
- Update integration expectations when behavior changes. Automated runs use
  the disposable VM matrix with a `smoke` or `local` profile; leave direct-host
  integration-harness execution to a human operator.

## Required Validation

When changing shell logic, run:

```sh
./tests/validate.sh quick [PATH...]
./tests/validate.sh full
```

`quick` runs the budget check, the changed modules' entry suites and the
contract suites; `full` runs the pinned lint stack (`./tests/run_lint.sh`),
every unit suite (`./tests/run_shunit_tests.sh`) and report-only bash-xtrace
coverage (`ZXFER_COVERAGE_MODE=bash-xtrace ./tests/run_coverage.sh`). When
changing one area heavily, run its entry suite first
(`./tests/run_shunit_tests.sh tests/test_zxfer_NAME.sh`), then the full unit
set before finishing.

## Documentation Expectations

When behavior, defaults, workflows, or public output change, review the related
docs in the same change:

- [../README.md](../README.md)
- [../CHANGELOG.txt](../CHANGELOG.txt)
- canonical [../man/zxfer.8](../man/zxfer.8), followed by
  `./tests/generate_solaris_manpage.sh --write` for the generated `.1m` page
- [testing.md](./testing.md)
- [architecture.md](./architecture.md)
- [../KNOWN_ISSUES.md](../KNOWN_ISSUES.md) when applicable
- packaging or workflow files when installation, CI, or release behavior moved

The style guide is not a substitute for judgment. When safety or portability is
at risk, prefer the clearer and more defensive implementation even if it is
slightly more verbose.
