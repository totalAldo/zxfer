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

## Correctness And Portability

### Medium: `-s` with `-Y` reuses the snapshot name on later passes

`zxfer_stamp_new_snapshot_name` in `src/zxfer_replication.sh` names the `-s`
snapshot `zxfer_<pid>_<YYYYmmddHHMMSS>` once per run, and every `-Y` pass's
`zxfer_newsnap` reuses that name. Because the first pass always sends the new
snapshot, a `-s -Y` run always starts a second pass, whose
`zfs snapshot -r SOURCE@<same name>` fails on real ZFS with "dataset already
exists", so the run exits non-zero after the first pass already replicated. Nothing is destroyed. The canned-zfs
black-box harness shows the repeated name (one `snapshot -r` per pass with the
same name) but does not model the failure. Choosing either one snapshot per
run or a fresh name per pass is an interface decision still to be made.

### Low: a dataset recreated during a recursive property read can take forged property values

With `-R` and `-P` or `-o`, each side's properties are read with two
`zfs get -r` value views and then a name list (`src/zxfer_property_state.sh`,
`zxfer_prefetch_recursive_normalized_properties` and
`ZXFER_PROPERTY_NORMALIZE_AWK`). If a dataset in the tree is destroyed before
the value views and recreated (or renamed away and back) before the name list,
the name list holds its properties while the views do not. The lines of the
value printed just before it can then pass for its records, native ones
included. That value normally belongs to the previous dataset's last user
property, and it is itself cut at its first line. The dataset and property
names always come from the name list, so a forged value can land only on a
property the recreated dataset really has.

The destroy and recreate does not have to be done by whoever wrote that value:
a third party or a churn job can do it, while anyone allowed to set the user
property (for example through `zfs allow userprop`) supplies the forged lines.
With `-P` (which `-k` and `-m` also turn on), zxfer then applies any forged
native property it does not treat as read-only to the destination copy of the
recreated dataset, with zxfer's privileges. Examples are `sharenfs`, `setuid`,
`exec`, `readonly`, `quota` and `canmount`, plus `mountpoint` under `-m`.
Those privileges can exceed what the writer of the value may set on the
source. With `-k` the forged values are also written to the backup metadata,
and a later `-e` restore applies them again. Under `-o` without `-P`, `-k` or
`-m`, zxfer takes only the dataset type, the volume size and the creation-time
properties (`casesensitivity`, `normalization`, `utf8only`) from the source,
so a forged value can change only how a missing destination dataset is
created; on an existing destination, a forged creation-time value that differs
from the destination's stops the run (exit 1) with the error that the property
`may only be set at filesystem creation time`.

It stays Low because it needs all of these: `-R` with `-P` (or `-k`/`-m`) or
`-o`; a dataset destroyed and recreated inside the gap between two of zxfer's
`zfs get -r` calls; and write access to the value printed before it. A
property removed between the calls is never listed. A property added between
them can take its record only from the value printed just before it in the
same dataset. For an added native property that is another native value,
never a user property's text; an added user property is the next entry.
Per-dataset reads (`-N`, and every dataset the prefetch leaves out) cannot
take another dataset's values.
`test_recursive_read_race_residual_forges_a_recreated_dataset` in
`tests/test_contract_properties.sh` pins the current behavior: a forged
`readonly=on` reaches `zfs set`. Reading the name list both before and after
the value views, and publishing nothing unless the two lists match, would cost
one more `zfs get -r` per side but only narrow the window: a dataset destroyed
after the first name list and recreated with the same property names before
the second still passes.

### Low: a user property created during a property read can cut the value before it and take its text

Every `zfs get all` read takes the machine and human value views first and the
property names alone last (`src/zxfer_property_state.sh`,
`ZXFER_PROPERTY_NORMALIZE_AWK`). A user property created between the value
views and the name list is listed while the views lack it, so the lines of the
value printed just before it (the carrier) can pass for its record. zfs prints
native properties before user properties, so a carrier whose text can forge a
record is always a user property (a created first user property follows the
last native value, which the permission to set user properties cannot write).
The read still succeeds and publishes three wrong things: the carrier's value
cut at its first line feed; a source for the carrier taken from its own text
(`received`, `default`, `inherited from X` or `-` instead of `local`, which
can make `-P` inherit or skip the carrier instead of setting it); and a value
and source for the created property, and for any property created right after
it, taken from that text. The cut stays even when the created property's line
is followed by more lines and it is re-read alone. zxfer exits 0, `-P` applies
the values with `zfs set` or `zfs create -o`, and `-k` writes them to the
backup metadata, which a later `-e` restore applies again.

It needs only the permission to set user properties on the source (for example
`zfs allow userprop`), or on the destination, whose read decides what `-P`
changes, plus timing one `zfs set` (or a set and inherit loop) between two of
zxfer's `zfs get` calls; the calls show in `ps`, repeat on every scheduled
run, and a miss is silent. It affects per-dataset reads (`-N`, and every
dataset the recursive prefetch leaves out) and the recursive prefetch, on both
sides, locally and over `-O`/`-T`. In a recursive read, a whole dataset
created between the calls (which needs the create permission, or a churn job)
cuts the last value printed before it the same way.

It stays Low because the published values touch only user properties, which
that permission can set directly anyway, although the replica can end with
values the source never held at any moment; native properties cannot be forged
this way.
`test_parse_property_views_residual_created_record_takes_text_from_the_value_before_it`
in `tests/test_zxfer_property_state.sh` pins the current
behavior. A complete fix reads every user property alone (two `zfs get` calls
per user property per dataset) or uses `zfs get -j` where OpenZFS 2.3 or later
provides it. Reading the name list, or the machine view, a second time costs
one more `zfs get` per read but only narrows the window: a property created
and removed again at the right moments still passes.

### Low: `-P` cannot set or inherit a user property whose name starts with `-`

OpenZFS accepts user property names that start with `-` (for example `-x:y`;
only a colon is required). zxfer reads such a name (a property read alone
names it after `--`) and creates it through `zfs create -o`. However,
`zxfer_apply_property_changes` in `src/zxfer_property_transfer.sh` passes it
as a bare `zfs set` or `zfs inherit` operand (`-x:y=VALUE` or `-x:y`), which
zfs parses as an option. A `-P` run
that must change or inherit such a property on an existing destination
therefore stops with
`Error when setting properties on destination filesystem.` or
`Error when inheriting properties on destination filesystem.` and zfs's exit
status. That dataset's properties are not changed, and later datasets are not
reconciled. This predates the name-list parser.

`--` is not a safe blanket fix for `zfs set`: older `zfs set` builds, illumos
included, reject any first argument that starts with `-`, `--` included.
Current OpenZFS parses `zfs set` options with getopt, and without `--` the
glibc getopt on Linux also takes a dash-named item later in the list as an
option, so putting a plain assignment first does not help. `zfs inherit`
parses its options with getopt everywhere, so `--` should work there, but that
is unverified. A fix needs a per-host capability test or a documented refusal.
`hostile_property_dash_name_test` stays off this path.

### Low: wrapper spawn mode cannot safely stop reaped or reparented background work

Without process-group isolation (no working `setsid` and no shell job control
that isolates jobs started from a subshell; for example FreeBSD and illumos,
Linux hosts whose `/bin/sh` is dash without `setsid`, and interactive macOS
runs with a terminal), zxfer tracks background work by PID through
`src/zxfer_cleanup_child_wrapper.sh`. FreeBSD sh and ksh93 isolate a job only
when the root shell starts it, and zxfer cannot cheaply tell a subshell spawn
apart, so without a `setsid` command FreeBSD and illumos use the wrapper for
every spawn:

- `zxfer_abort_all_send_jobs` in `src/zxfer_send_jobs.sh` sends no teardown
  signal to a job that has written its status file, so an abort does not stop
  a descendant that outlived the job shell. A job shell that died without
  writing its status still gets a bare-PID TERM and a wrapper STOP/KILL; if the
  shell already reaped it and the PID was reused in that window (milliseconds),
  an unrelated process is signalled.
- A failed source discovery producer in `src/zxfer_snapshot_discovery.sh` can
  leave reparented pipeline stages (`zfs list`, `sort`, `tee`) running. zxfer
  no longer signals them, because the reaped PID may be recycled; they may
  outlive zxfer and write into the private temp root, which trap cleanup
  removes without waiting for them.

In process-group mode the group of a status-written job is still signalled;
that reaches an unrelated group only if every member exited and a new group
leader reused the PID. In both modes, the `kill -0` liveness check in
`zxfer_wait_for_any_send_job` for a job with no status can be fooled by a
recycled PID, so the wait keeps polling until the PID exits.

### Low: process-group statuses are wrong under `posh`, and a BusyBox userland is unvalidated

`zxfer_signal_process_group` in `src/zxfer_exec.sh` sends every group probe
and signal as `kill -SIG -PGID`, the one form dash, bash, ksh93, zsh, FreeBSD
sh and BusyBox ash all read. `posh` has no `kill` builtin, so it runs
procps-ng's `/usr/bin/kill`, and procps-ng 4.0.4 (Ubuntu 26.04) inverts the
exit status of that form: 0 for a group that is gone and 1 for a live one.
The signals are still delivered, but under `posh` zxfer misreads whether a
group is alive, so cleanup registration can drop a live group and group
signal results are wrong. The main-only `posh` CI lane is non-blocking and
fails the process-group suites for this reason.

BusyBox ash itself is supported, but a host whose whole userland is BusyBox
(Alpine-like) is unvalidated: BusyBox `ps` rejects `-p`, which wrapper-mode
teardown and the setsid launcher check in `zxfer_signal_background_shell`
use. Those checks fail closed rather than signal the wrong process.

### Low: GNU `stat` special bits fail the error-log lock directory check under a setgid parent

On Linux, GNU `stat -c %a` also prints the set-user-ID, set-group-ID and
sticky bits (for example `2700`), and a directory created inside a
set-group-ID directory inherits that bit. `zxfer_get_path_mode_octal` in
`src/zxfer_path_security.sh` passes that value through, and the
`ZXFER_ERROR_LOG` owned-lock directory check (`zxfer_validate_owned_lock_path`
in `src/zxfer_error_log.sh`) compares it with exactly `700`. Under a
set-group-ID log parent, zxfer warns and does not mirror the report; this
fails closed. The per-run temp root accepts `[1-7]700` since 2026-09-23, and
the short fallback ssh socket directory is no longer revalidated after
creation. BSD `stat -f %OLp` prints permission bits only, so FreeBSD and macOS
are not affected. Found by review; not yet reproduced on a Linux host.

### Low: an internal error in the startup capability probe can exit without a message

Outside `-v`/`-V`, `zxfer_preload_remote_host_capabilities` in
`src/zxfer_remote_hosts.sh` runs the `-O`/`-T` startup capability probe with
stdout and stderr sent to `/dev/null`, so that an ordinary probe failure stays
quiet until a later lookup reports it. If something inside the probe throws
instead of returning, such as a failed scratch-file allocation, the run still
stops with a non-zero status before any replication work, but its message is
discarded, and under bash (macOS `/bin/sh`) so is the structured failure
report. Rerun with `-v` to see the diagnostic. Found by review; it is the same
pattern as the `-O -j` parallel lookup fixed on 2026-09-24, not reproduced
separately.

### Low: SJIS/Big5/GBK trail bytes can hide a backslash in report quoting

The failure-report and `-v` quoting fast path prints a printable token as-is
when the shell sees no backslash or single quote in it. In a double-byte
locale such as `ja_JP.SJIS` or `zh_TW.Big5`, a character whose trail byte is
0x5C is one character to the shell, so that byte is printed raw instead of
being doubled as the old `awk` path under `LC_ALL=C` did. No control byte or
unbalanced quote gets through (fuzzed across bash, dash, ksh, and `/bin/sh`),
so the risk is ambiguous log text, not terminal injection. See
`zxfer_quote_token_for_report` in `src/zxfer_reporting.sh`.

### Low: report escaping passes C1 controls, and the `-U` warning prints values raw

`zxfer_escape_report_value` (`src/zxfer_reporting.sh`) escapes C0 control
bytes and DEL, but its `LC_ALL=C` `awk` passes every byte from 0x80 up
unchanged. A C1 control character, UTF-8 encoded (`C2 80` to `C2 9F`, such as
`C2 9B`, the 8-bit CSI) or as a raw byte, therefore reaches structured failure
reports, `ZXFER_ERROR_LOG` and the `-v`/`-V` property lines as is, and some
terminals (xterm, for example) act on it. This predates the 2026.09.26
verbose escaping. Separately, with `-U` and `-v`, the warning
`Destination does not support property NAME=VALUE` (`ZXFER_PROPERTY_FILTER_AWK`
and `zxfer_plan_property_changes` in `src/zxfer_property_transfer.sh`) prints
the decoded value raw, one warning line per line of the value, so a value can
print C0 control bytes and forge output lines. User properties are never
marked unsupported, so the permission to set user properties alone cannot
reach that warning; it needs write access to a native property on the source,
or a hostile `-O` host. A fix would escape the UTF-8 sequences `C2 80` to
`C2 9F` in the report escaper, and keep the `-U` warning's value encoded in
`awk` and print it through `zxfer_escape_report_value`.

### Low: parallel shunit output can hide a stalled suite until workflow timeout

`tests/run_shunit_tests.sh` buffers parallel worker logs and replays them in
suite order, but it does not impose a per-suite deadline or dump pending worker
state when a suite stalls. A process-supervision regression can therefore hide
later completed suites and hold CI until the outer 30- or 60-minute workflow
timeout. The process-heavy tests now avoid known command-substitution pipe
leaks and CI uses bounded worker counts, but the runner still needs portable
per-suite timeout and pending-log diagnostics so future failures terminate with
actionable evidence.

### Low: shunit signal cleanup waits when all process identity is unavailable

Signal cleanup also fails closed if a host provides neither usable process-start
tokens nor any supported parent/child enumeration. In that degraded state the
runner keeps its wrapper alive instead of risking a reused PID or orphaned test
descendant, and ignores repeated catchable signals after teardown begins. A
manual `KILL` remains possible, but can orphan the unverified child. Supported
CI platforms provide at least the parent/child path; the degraded path emits an
explicit diagnostic rather than silently targeting an unverified process.

### Resolved: silent destroy/rollback/resend churn on GUID-diverged destinations (fixed 2026-06-12)

Before 2026-06-12, when destination snapshots matched source snapshots by
NAME but carried different GUIDs (diverged data under identical names), a
`-d` run silently destroyed those destination snapshots, rolled the
destination back to the last GUID-matching common snapshot, and re-sent the
whole range — on every run, with no operator messaging. The legacy name-only
matching variant of the same fixture was silent in the opposite direction: it
reported "No new snapshots to transfer" and treated diverged data as in sync.

This is resolved by the divergence contract (see `README.md` and the `-d`/`-F`
entries in `man/zxfer.8`): an always-on stderr warning names the diverged
dataset, the count, and example snapshots with both GUIDs; destructive
convergence requires BOTH `-d` and `-F` (otherwise the run fails closed with
zero actions for the diverged dataset); and a post-receive verification of the
live destination listing turns any re-divergence into a structured error
naming the snapshot. Regression coverage:
`tests/test_contract_planning.sh` (divergence contract pins) and
`tests/test_zxfer_snapshot_reconcile.sh` (classifier, gate, and verification
units).
