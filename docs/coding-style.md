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
- Reserve `*_runtime` for process lifecycle, temp resources, traps, and
  cleanup.
- Keep tests aligned with module ownership where practical:
  `tests/test_<peer>.sh`.

## Naming

- Shared helper functions should use the `zxfer_` prefix.
- Keep function names descriptive and action-oriented:
  `zxfer_render_*`, `zxfer_validate_*`, `zxfer_ensure_*`,
  `zxfer_reset_*`, `zxfer_get_*`, `zxfer_write_*`.
- Global shell state should use the existing `g_` prefix only for mutable
  runtime or session state.
- Parsed option state should use `g_option_*` and should not be reused as
  general scratch state.
- Function-scoped temporaries should use `l_` prefixes consistently.
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
- Reuse the centralized command-rendering and execution helpers in
  [../src/zxfer_exec.sh](../src/zxfer_exec.sh) instead of adding new ad hoc
  `eval` paths.
- Treat `-O` / `-T` host specs and remote wrapper tokens as structured command
  inputs, not as plain hostnames.

## Dependency And Path Handling

- Resolve required tools through
  [../src/zxfer_dependencies.sh](../src/zxfer_dependencies.sh).
- Preserve the secure-PATH model and do not bypass it with unvalidated bare
  `PATH` lookups in feature code.
- Keep remote helper resolution inside
  [../src/zxfer_remote_hosts.sh](../src/zxfer_remote_hosts.sh).
- Validate absolute paths and reject control characters or unsafe whitespace in
  resolved helper paths.

## Errors, Logging, And Output

- Route operator-facing failures through the reporting helpers in
  [../src/zxfer_reporting.sh](../src/zxfer_reporting.sh).
- Preserve structured stderr failure reporting, failure classes, and failure
  stages.
- Prefer existing output helpers such as `zxfer_echov`, `zxfer_echoV`, and `zxfer_throw_error*`
  instead of printing new ad hoc messages.
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
  `zxfer_create_runtime_artifact_dir`,
  `zxfer_write_runtime_artifact_file`,
  `zxfer_read_runtime_artifact_file`,
  `zxfer_write_runtime_cache_file_atomically`,
  `zxfer_cleanup_runtime_artifact_path`.
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
- Register background PIDs and cleanup artifacts with the existing runtime
  helpers.
- Remove temporary files, FIFOs, queues, and cache directories on both success
  and failure paths unless they are intentionally preserved for debugging.
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
# Purpose: Return the resolved ssh transport argv for one host spec.
# Usage: Called during remote bootstrap and command rendering so callers reuse
# one quoting-safe transport builder instead of rebuilding ssh argv by hand.
# Returns: Newline-delimited transport tokens.
zxfer_get_ssh_transport_tokens_for_host() {
```

- Use `Purpose:` and `Usage:` on every function comment block.
- Apply this requirement to top-level shell functions defined directly in
  `src/` modules. Function literals embedded inside generated shell payload
  strings are not source-level API helpers; document the enclosing builder
  function instead.
- Add `Returns:` or `Side effects:` only when stdout contracts, exit-status
  meaning, global mutation, locking, staging, or cleanup behavior would not be
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
- Use [../tests/test_helper.sh](../tests/test_helper.sh) for shared scaffolding
  before adding new suite-local plumbing.
- Keep fixtures explicit and local to the suite unless they are broadly useful.
- Update the integration harness expectations when behavior changes, but leave
  actual integration execution to a human operator.

## Required Validation

When changing shell logic, run:

```sh
./tests/run_shunit_tests.sh
./tests/run_lint.sh
ZXFER_COVERAGE_MODE=bash-xtrace ./tests/run_coverage.sh
```

When changing one area heavily, run the focused suite for that module first,
then the full unit set before finishing.

## Documentation Expectations

When behavior, defaults, workflows, or public output change, review the related
docs in the same change:

- [../README.md](../README.md)
- [../CHANGELOG.txt](../CHANGELOG.txt)
- [../man/zxfer.8](../man/zxfer.8)
- [../man/zxfer.1m](../man/zxfer.1m)
- [testing.md](./testing.md)
- [architecture.md](./architecture.md)
- [../KNOWN_ISSUES.md](../KNOWN_ISSUES.md) when applicable
- packaging or workflow files when installation, CI, or release behavior moved

The style guide is not a substitute for judgment. When safety or portability is
at risk, prefer the clearer and more defensive implementation even if it is
slightly more verbose.
