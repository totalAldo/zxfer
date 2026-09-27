# Security

## Scope

zxfer runs privileged filesystem and replication commands and often needs
either root, delegated ZFS privileges, or privileged remote wrappers. That
makes shell quoting, remote execution, file ownership checks, and dependency
resolution security-sensitive by default.

## Current Security Model

Key protections already present in the project include:

- secure-PATH resolution for required local helpers and the main remote helper
  lookups (`zfs`, `cat`, and GNU `parallel`), with resolved helper paths
  rejected if they contain a tab, carriage return, or line feed; local helpers
  resolve only to executable regular files on the secure PATH, so shell
  functions, aliases, and builtins never satisfy a lookup, and an empty secure
  PATH is refused rather than exported
- rejection of tab, carriage-return, or line-feed bytes in
  `ZXFER_SECURE_PATH`, `ZXFER_SECURE_PATH_APPEND`, and `ZXFER_BACKUP_DIR`
  before those values are split, cached, exported, or remotely rendered, and
  of any control character in the source and destination operands
- structured failure reporting instead of ad hoc error handling
- safe-by-default failure-report redaction for `invocation` and `last_command`
- hardened `ZXFER_ERROR_LOG` path validation
- secured property backup metadata directories and file-permission checks
- explicit handling for wrapped remote host specs
- separate argv-preserving execution and hardened rendered-pipeline APIs;
  remote zfs commands over `-O`/`-T` are rendered argument by argument, so a
  newline or an empty argument reaches the remote zfs intact
- source property values are treated as untrusted: they are percent-encoded
  internally and decoded in the shell into exactly one argv entry per
  property, with no separator byte, so no byte in a value (such as `\001` or a
  newline) can become an extra `zfs create -o` or `zfs set` argument; every
  `zfs get all` read also lists the property names alone and takes a value
  line only where that list pins it to one whole record, reading any other
  property alone (its name after `--`, so a name that starts with `-` is never
  taken as an option), so a value that prints like another record can neither
  be cut short nor add a property (two narrow races remain, both in
  [KNOWN_ISSUES.md](./KNOWN_ISSUES.md): in a recursive read, a dataset
  destroyed and recreated between two `zfs get -r` calls can take forged
  values, native properties included; and in any read, a user property
  created between the value views and the name list can cut the user
  property value printed before it and take its value and source from that
  text. A user property removed before its lone re-read, which zfs reports
  with source `-`, is left out, as if the read had begun after the removal);
  property lists reach `awk` only through `ENVIRON`, never `awk -v`, so
  backslash escapes are never reinterpreted, and each property `awk` call sets
  every `ZXFER_AWK_*` variable it reads, so the operator's environment cannot
  change filtering
- ssh-backed commands inside zxfer's read loops (such as the batched `-U`
  type scan and the `-d` creation-time query that `-g` depends on) read
  `/dev/null`, so a remote command cannot consume the loop's input
- readable multiline capability and remote-backup directory/write/read and
  `-O` storage-listing programs with golden protocol pins, all under the same
  secure PATH and symlink guard; long or multiline programs cross csh/tcsh login
  shells as bounded, quoted positional chunks on one physical command line,
  then a fixed POSIX `sh` bootstrap reassembles the exact bytes before the
  explicit `sh -c` while preserving stdin and status
- a private per-run 0700 artifact root whose exact path, validated parent, and
  stored inode, owner and 0700 mode record must match runtime-owned
  provenance before recursive cleanup; owner, mode and inode come from one
  `ls -ldin` line, whose fields GNU coreutils, the BSDs, macOS, illumos and
  BusyBox print alike, so every platform applies the same checks
- remote (`-T`) destination discovery runs no target-side script: each
  destination `zfs list` is one argv-quoted command over the role's control
  master, and its own exit status (255 when ssh loses the connection) decides
  whether its output file is used, exactly as for a local listing; a failed
  listing falls back only to the exact existence probe, never to partial
  discovery state, and a listing ssh could not deliver stops the run without
  even that probe
- remote capability responses are framed, coverage-checked, and parsed once per
  host and requested tool set (equal `-O` and `-T` specs share one probe);
  only a fully validated response is stored, and later OS/tool lookups reuse
  its validated fields instead of trusting or reparsing raw handshake text
- one run-private directory outside the run root, the ssh short socket
  directory made only when a long TMPDIR would push the control-socket path
  past the `sun_path` limit: a random `mktemp -d` name under the validated
  default temp root, removed at exit without recursion (the two role sockets
  and ssh's temporary listener names, then the empty directory), never
  through a symlink
- pre-trap rejection of inherited internal cleanup handles, so exported `g_*`
  state cannot authorize process signals, SSH actions, path removal, or SMF
  service changes
- inherited snapshot scratch-file paths (the live destination view and
  depth-1 listing files) are reused only when they lie under the run's private
  temp root, and an inherited `g_cmd_awk` is cleared before the launcher
  records the invocation
- background-job teardown restricted to registered zxfer jobs, using verified
  process groups or a wrapper with start-token checks for descendant cleanup;
  a job that has recorded its exit status is signalled only through its
  process group, and the process-group path signals a bare PID only while it
  is still in zxfer's own process group, so a recycled PID is not signalled
  there; wrapper mode keeps a narrow PID-reuse window described in
  [KNOWN_ISSUES.md](./KNOWN_ISSUES.md)

Structured failure reports now redact `invocation` and `last_command` as
`[redacted]` by default in both `stderr` output and any `ZXFER_ERROR_LOG`
mirror, so routine logs and wrappers do not capture raw command lines. If an
operator explicitly wants verbatim command text during local debugging, they
can set `ZXFER_UNSAFE_FAILURE_REPORT_COMMANDS=1`; doing so is unsafe because
wrapper arguments, hook strings, or other command-line fragments may then be
written to `stderr` and `ZXFER_ERROR_LOG`. Raw ASCII control bytes in
structured failure-report values are escaped before output so terminal and
pager control sequences cannot execute from report fields. The `-v`/`-V`
property lines (the `Property set list`/`Property inherit list` lines, the
`zxfer_transfer_properties` list dumps, and the rendered `zfs set` or
`zfs inherit` line, locally and over `-T`) escape property values the same way,
and verbose output is printed with `printf`, so no shell's `echo` can turn an
escaped `\033` or `\c` back into a control byte. Two gaps remain (see
[KNOWN_ISSUES.md](./KNOWN_ISSUES.md)): C1 control characters (bytes
0x80-0x9F, raw or UTF-8 encoded) pass through this escaping, and the `-U`
unsupported-property warning prints its value raw.

`ZXFER_ERROR_LOG` mirroring checks the path before every append: the path is
absolute with no symlinked component, the parent is owned by root or the
effective user and not writable by others unless it is sticky, and the log is
a regular 0600 file with a single link, owned by root or the effective user.
The single-link rule refuses a root-owned 0600 file that another user
hard-linked into a shared parent. A missing log is created under umask 077
with noclobber, so zxfer never truncates or replaces an existing file, and is
set to mode 0600 only after it passes the other checks, so that chmod never
reaches what another user put at the name. Each report is then appended with
one `O_APPEND` write, without a lock. Two guards
of earlier versions are given up. First, zxfer no longer pins the validated
file while it writes, so root or the effective user can swap the path between
the checks and the write and send the report elsewhere; other users cannot,
because they cannot replace an entry in a parent that passes the checks.
Second, reports from concurrent runs can interleave on NFS, whose appends are
not atomic, and when one report needs more than one write (larger than the
file system block size, often 4 KiB; a default report is a few hundred bytes).
In a shared sticky directory such as `/tmp`, another user can still create the
log's name first. zxfer then refuses the log rather than write to it, but a
FIFO created there just as zxfer creates the log holds the failing run's exit
until someone opens the FIFO (zxfer still writes nothing to it). Keep the log
in a directory only root or the zxfer user can write.

Current open security concerns are tracked in [KNOWN_ISSUES.md](./KNOWN_ISSUES.md).

## Reporting A Vulnerability

Please do not open a public issue for a suspected command-injection, trust-
boundary, privilege-escalation, or data-destruction vulnerability until the
maintainer has had a chance to assess it privately.

Send private vulnerability reports to:

- `zxfer@totalaldo.com`

If possible, include:

- affected command line
- platform and shell
- exact stderr output
- whether the issue is local, remote, or backup-metadata related
- minimal reproduction steps

## Security Review Hotspots

Changes in these areas should receive extra scrutiny:

- remote command construction
- any `eval` usage
- secure-PATH resolution
- property backup / restore lookup
- ssh control-socket management
- runtime-root and ssh socket-directory cleanup
- background-process registration, signalling, and status protocols
- snapshot deletion and rollback behavior
