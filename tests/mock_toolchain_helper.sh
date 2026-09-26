#!/bin/sh
#
# Shared mock-toolchain helpers for black-box driving ./zxfer with a canned
# zfs, a mock ssh and counting wrappers. Sourced by shunit2 suites and bench
# runners:
#   tests/test_zxfer_mock_toolchain.sh (self-test)
#   tests/helpers/blackbox.sh (black-box suites)
#   tests/run_microbench.sh / tests/test_zxfer_microbench_budgets.sh
#   tests/run_perf_ab.sh
#
# Standalone by design: no dependency on the integration harness. The only
# host requirements are POSIX sh plus the standard userland already required
# by zxfer itself.
#
# Canned zfs runtime environment (read by the generated mock zfs script):
#   MOCK_ZFS_LOG            append-only argv log; one invocation per line,
#                           argv joined with single spaces, mutating
#                           subcommands prefixed with "MUTATE ", and each
#                           receive followed by an "END receive <dataset>"
#                           line when its stream ends. Unset or empty
#                           disables logging.
#   MOCK_ZFS_FIXTURE_DIR    directory containing "manifest" plus fixture
#                           files for read-only discovery answers.
#   MOCK_ZFS_DEFAULT_STATUS exit status for unmatched read-only commands
#                           (default 1).
#
# Manifest format ($MOCK_ZFS_FIXTURE_DIR/manifest), one rule per line:
#   <glob-pattern><TAB><fixture-file><TAB><exit-status>[<TAB>once]
# The pattern is matched (sh case glob, first match wins) against the argv
# key: all zfs arguments joined with single spaces, e.g.
#   "list -Hr -o name,guid -s creation -t snapshot srcpool/data".
# <fixture-file> is relative to $MOCK_ZFS_FIXTURE_DIR; "-" emits no output.
# <exit-status> defaults to 0 when omitted. Lines starting with "#" and
# blank lines are skipped. "|" alternation is not supported; use one
# pattern per line.
# The optional 4th field "once" marks a consumable rule: after its first
# match the mock rewrites the manifest without that line, so the next lookup
# for the same argv falls through to a later rule. This is the only stateful
# manifest feature (the manifest lives in the per-case scratch state dir, so
# the rewrite is safe); use it to answer the same query differently before
# and after a mutation, e.g. a diverged-guid listing healed by a receive.
# Consumable rules are not safe under concurrent mock invocations.
#
# Counting wrapper runtime environment:
#   MOCK_SPAWN_LOG          append-only spawn log; one tool name per line.
#                           Unset logs to /dev/null (no counting overhead).
#
# Socket-aware mock ssh runtime environment (zxfer_mockbin_write_socket_ssh):
#   MOCK_SSH_LOG            append-only "<kind><TAB><argv>" log, one line per
#                           invocation; summarize it with
#                           zxfer_mockbin_ssh_log_rows.
#   MOCK_SSH_HANDSHAKE_S    seconds each new connection sleeps (unset: none).
#   MOCK_SSH_MUX_S          seconds each multiplexed or control call sleeps.
#
# shellcheck shell=sh

# Fixed fixture topology used by zxfer_mockbin_build_fixture_tree. Constants
# so suites and bench runners reference one source of truth for the dataset
# names the manifests are keyed on.
ZXFER_MOCKBIN_SOURCE_ROOT="srcpool/data"
ZXFER_MOCKBIN_DEST_ROOT="dstpool/back"
ZXFER_MOCKBIN_DEST_MAPPED_ROOT="dstpool/back/data"
ZXFER_MOCKBIN_SYSTEM_SECURE_PATH="/sbin:/bin:/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin"

# Purpose: Resolve one host tool to an absolute path from the caller's PATH.
# Usage: Internal lookup for zxfer_mockbin_prepare_dir and counting-wrapper
# callers that need the real binary location before shadowing it.
# Returns: Absolute path on stdout, or status 1 with a stderr note.
zxfer_mockbin_resolve_host_tool() {
	l_mockbin_tool=$1

	l_mockbin_resolved=$(command -v "$l_mockbin_tool" 2>/dev/null || :)
	case "$l_mockbin_resolved" in
	/*)
		printf '%s\n' "$l_mockbin_resolved"
		return 0
		;;
	esac

	printf 'mock_toolchain_helper: required host tool not found: %s\n' \
		"$l_mockbin_tool" >&2
	return 1
}

# Purpose: Symlink real host tools into a mock bin directory so zxfer can
# resolve them through a secure PATH that leads with that directory.
# Usage: zxfer_mockbin_prepare_dir <dir> <real-tool>... — creates <dir> when
# missing and replaces existing entries, so it is safe to call repeatedly or
# after dropping a canned zfs into the same directory.
zxfer_mockbin_prepare_dir() {
	l_mockbin_dir=$1
	shift

	mkdir -p "$l_mockbin_dir" || return 1

	for l_mockbin_entry in "$@"; do
		l_mockbin_real=$(zxfer_mockbin_resolve_host_tool "$l_mockbin_entry") ||
			return 1
		ln -sf "$l_mockbin_real" "$l_mockbin_dir/$l_mockbin_entry" || return 1
	done
}

# Purpose: Write the canned zfs mock that logs every invocation and answers
# read-only discovery from a manifest-driven fixture directory.
# Usage: zxfer_mockbin_write_canned_zfs <path> — typically
# <mockdir>/zfs so the secure PATH resolves it ahead of any real zfs.
# Side effects: Overwrites <path> and marks it executable. Behavior of the
# generated script:
#   - every argv is appended to $MOCK_ZFS_LOG (space-joined, one line);
#   - destroy/rollback/create/set/inherit/snapshot/rename/clone/promote/
#     hold/release/bookmark are logged with a "MUTATE " prefix and exit per
#     manifest (default 0);
#   - receive/recv is logged without the MUTATE prefix, consumes stdin to
#     /dev/null, logs "END receive <dataset>" (its last argument) once the
#     stream ends, and exits per manifest (default 0);
#   - send emits matched fixture bytes, or a one-line dummy stream when
#     unmatched, and exits per manifest (default 0);
#   - all other subcommands are read-only: matched rules emit the fixture
#     and exit with the rule status; unmatched commands print a stderr note
#     and exit $MOCK_ZFS_DEFAULT_STATUS (default 1).
zxfer_mockbin_write_canned_zfs() {
	l_mockbin_zfs_path=$1

	cat >"$l_mockbin_zfs_path" <<'EOF'
#!/bin/sh
# Canned zfs mock generated by tests/mock_toolchain_helper.sh.
# See that helper's header comment for the manifest and env contract.

mock_key=$*
mock_fixture=""
mock_status=0

mock_log_line() {
	if [ -n "${MOCK_ZFS_LOG:-}" ]; then
		printf '%s\n' "$1" >>"$MOCK_ZFS_LOG"
	fi
}

# First manifest rule whose glob pattern matches the space-joined argv wins.
# A matching rule whose optional 4th field is "once" is consumed: the
# manifest is rewritten without that line before returning, so the next
# lookup for the same argv falls through to a later rule.
mock_manifest_lookup() {
	[ -n "${MOCK_ZFS_FIXTURE_DIR:-}" ] || return 1
	mock_manifest="$MOCK_ZFS_FIXTURE_DIR/manifest"
	[ -r "$mock_manifest" ] || return 1
	mock_tab=$(printf '\t')
	mock_line_no=0
	while IFS=$mock_tab read -r mock_pattern mock_rule_fixture mock_rule_status mock_rule_flag; do
		mock_line_no=$((mock_line_no + 1))
		case "$mock_pattern" in
		'' | '#'*)
			continue
			;;
		esac
		# shellcheck disable=SC2254  # manifest patterns glob-match on purpose
		case "$mock_key" in
		$mock_pattern)
			mock_fixture=$mock_rule_fixture
			mock_status=${mock_rule_status:-0}
			if [ "$mock_rule_flag" = "once" ]; then
				awk -v consumed_line="$mock_line_no" 'NR != consumed_line' \
					"$mock_manifest" >"$mock_manifest.consumed" &&
					mv "$mock_manifest.consumed" "$mock_manifest"
			fi
			return 0
			;;
		esac
	done <"$mock_manifest"
	return 1
}

mock_emit_fixture() {
	if [ -n "$mock_fixture" ] && [ "$mock_fixture" != "-" ]; then
		cat "$MOCK_ZFS_FIXTURE_DIR/$mock_fixture"
	fi
}

case "${1:-}" in
destroy | rollback | create | set | inherit | snapshot | rename | clone | promote | hold | release | bookmark)
	mock_log_line "MUTATE $mock_key"
	if mock_manifest_lookup; then
		mock_emit_fixture
		exit "$mock_status"
	fi
	exit 0
	;;
receive | recv)
	mock_log_line "$mock_key"
	cat >/dev/null
	for mock_arg in "$@"; do
		mock_receive_dataset=$mock_arg
	done
	mock_log_line "END receive $mock_receive_dataset"
	if mock_manifest_lookup; then
		mock_emit_fixture
		exit "$mock_status"
	fi
	exit 0
	;;
send)
	mock_log_line "$mock_key"
	if mock_manifest_lookup; then
		mock_emit_fixture
		exit "$mock_status"
	fi
	printf 'ZXFERMOCKSTREAM %s\n' "$mock_key"
	exit 0
	;;
*)
	mock_log_line "$mock_key"
	if mock_manifest_lookup; then
		mock_emit_fixture
		exit "$mock_status"
	fi
	printf 'mock zfs: no manifest match for: %s\n' "$mock_key" >&2
	exit "${MOCK_ZFS_DEFAULT_STATUS:-1}"
	;;
esac
EOF
	chmod +x "$l_mockbin_zfs_path"
}

# Purpose: Write a minimal counting wrapper that records one spawn per
# invocation and then execs the real tool unchanged.
# Usage: zxfer_mockbin_write_counting_wrapper <path> <real-tool-abs-path> —
# the logged name is the basename of <path>; place the wrapper in the mock
# bin directory so it shadows the system tool via the secure PATH.
zxfer_mockbin_write_counting_wrapper() {
	l_mockbin_wrapper_path=$1
	l_mockbin_wrapper_real=$2

	case "$l_mockbin_wrapper_real" in
	/*) ;;
	*)
		printf 'zxfer_mockbin_write_counting_wrapper: real tool path must be absolute: %s\n' \
			"$l_mockbin_wrapper_real" >&2
		return 1
		;;
	esac
	if [ ! -x "$l_mockbin_wrapper_real" ]; then
		printf 'zxfer_mockbin_write_counting_wrapper: real tool not executable: %s\n' \
			"$l_mockbin_wrapper_real" >&2
		return 1
	fi

	l_mockbin_wrapper_name=${l_mockbin_wrapper_path##*/}
	{
		printf '#!/bin/sh\n'
		# shellcheck disable=SC2016  # MOCK_SPAWN_LOG must expand at wrapper runtime
		printf 'printf '\''%%s\\n'\'' "%s" >>"${MOCK_SPAWN_LOG:-/dev/null}"\n' \
			"$l_mockbin_wrapper_name"
		printf 'exec "%s" "$@"\n' "$l_mockbin_wrapper_real"
	} >"$l_mockbin_wrapper_path"
	chmod +x "$l_mockbin_wrapper_path"
}

# Purpose: Write a minimal mock ssh without control-socket support: `-M` is
# rejected as an unknown option, other option tokens are skipped, the host
# token is dropped, and the remaining arguments are joined with spaces and
# executed locally through `sh -c`, so commands resolve helpers (including the
# canned zfs) from the inherited PATH.
# Usage: zxfer_mockbin_write_minimal_ssh <path>. The generated script appends
# each invocation's argv to $MOCK_SSH_LOG when set. Drive zxfer with
# PATH/ZXFER_SECURE_PATH pointing at the mock dir first so "remote" commands
# stay inside the mock toolchain.
zxfer_mockbin_write_minimal_ssh() {
	l_mockbin_ssh_path=$1

	cat >"$l_mockbin_ssh_path" <<'EOF'
#!/bin/sh
[ -n "${MOCK_SSH_LOG:-}" ] && printf '%s\n' "$*" >>"$MOCK_SSH_LOG"
while [ $# -gt 0 ]; do
	case "$1" in
	-M)
		printf '%s\n' 'ssh: illegal option -- M' >&2
		exit 255
		;;
	-o) shift 2 ;;
	-*) shift ;;
	*) break ;;
	esac
done
shift
[ $# -gt 0 ] || exit 0
exec sh -c "$*"
EOF
	chmod +x "$l_mockbin_ssh_path"
}

# Purpose: Write a mock ssh with control-socket semantics that logs whether
# each invocation opened a new connection, so benches can budget connections.
# Usage: zxfer_mockbin_write_socket_ssh <path>. Each call appends
# "<kind><TAB><argv>" to $MOCK_SSH_LOG, where kind is:
#   version  the `ssh -M -V` support probe (no host);
#   master   `-M -S SOCKET`: creates SOCKET as a plain file and exits, as a
#            real `-fN` master does once it is up;
#   control  `-O check|exit` over SOCKET; exit removes it;
#   mux      a command over a live SOCKET;
#   direct   a command without -S, or whose SOCKET is not live (real ssh then
#            connects on its own).
# master and direct are new connections and sleep $MOCK_SSH_HANDSHAKE_S when
# set; mux and control calls sleep $MOCK_SSH_MUX_S when set. Options that
# take a separate value in OpenSSH (-o, -p, -i, -l, -F, -J ...) skip it, and
# the first operand is the host. Commands drop the host token and run
# locally through `sh -c`, so "remote" helpers resolve from the inherited
# mock PATH.
zxfer_mockbin_write_socket_ssh() {
	l_mockbin_ssh_path=$1
	# Resolved now so the mock never runs (and counts) a wrapped rm.
	l_mockbin_ssh_rm=$(zxfer_mockbin_resolve_host_tool rm) || return 1

	{
		printf '#!/bin/sh\n'
		printf "mock_rm='%s'\n" "$l_mockbin_ssh_rm"
		cat <<'EOF'
mock_argv=$*
mock_socket=""
mock_op=""
mock_master=0
mock_version=0
while [ $# -gt 0 ]; do
	case "$1" in
	-M) mock_master=1 ;;
	-V) mock_version=1 ;;
	-S)
		mock_socket=$2
		shift
		;;
	-O)
		mock_op=$2
		shift
		;;
	-B | -b | -c | -D | -E | -e | -F | -I | -i | -J | -L | -l | -m | -o | -P | -p | -Q | -R | -W | -w)
		shift
		;;
	-*) ;;
	*) break ;;
	esac
	shift
done

if [ "$mock_version" -eq 1 ] && [ $# -eq 0 ]; then
	mock_kind=version
elif [ "$mock_master" -eq 1 ] && [ -n "$mock_socket" ]; then
	mock_kind=master
elif [ -n "$mock_op" ]; then
	mock_kind=control
elif [ -n "$mock_socket" ] && [ -e "$mock_socket" ]; then
	mock_kind=mux
else
	mock_kind=direct
fi
[ -z "${MOCK_SSH_LOG:-}" ] ||
	printf '%s\t%s\n' "$mock_kind" "$mock_argv" >>"$MOCK_SSH_LOG"

case $mock_kind in
master | direct) mock_delay=${MOCK_SSH_HANDSHAKE_S:-} ;;
mux | control) mock_delay=${MOCK_SSH_MUX_S:-} ;;
*) mock_delay="" ;;
esac
[ -z "$mock_delay" ] || sleep "$mock_delay"

case $mock_kind in
version)
	printf '%s\n' 'OpenSSH_mock' >&2
	exit 0
	;;
master)
	: >"$mock_socket" || exit 255
	exit 0
	;;
control)
	if [ ! -e "$mock_socket" ]; then
		printf 'Control socket connect(%s): No such file or directory\n' \
			"$mock_socket" >&2
		exit 255
	fi
	[ "$mock_op" != exit ] || "$mock_rm" -f "$mock_socket"
	exit 0
	;;
esac
[ $# -gt 1 ] || exit 0
shift
exec sh -c "$*"
EOF
	} >"$l_mockbin_ssh_path" || return 1
	chmod +x "$l_mockbin_ssh_path"
}

# Purpose: Summarize a zxfer_mockbin_write_socket_ssh log as three TSV rows:
# ssh_connections (master opens plus direct commands), ssh_invocations (every
# call, probes and control calls included) and ssh_master_opens.
# Usage: zxfer_mockbin_ssh_log_rows <scenario> <ssh-log>. Lines that do not
# start with a known kind (continuations of an argv holding a newline) are
# skipped.
zxfer_mockbin_ssh_log_rows() {
	awk -F '\t' -v scenario="$1" '
		$1 ~ /^(version|master|control|mux|direct)$/ {
			calls++
		}
		$1 == "master" {
			masters++
		}
		$1 == "master" || $1 == "direct" {
			connections++
		}
		END {
			printf "%s\tssh_connections\t%d\n", scenario, connections
			printf "%s\tssh_invocations\t%d\n", scenario, calls
			printf "%s\tssh_master_opens\t%d\n", scenario, masters
		}
	' "$2"
}

# Purpose: Emit name<TAB>guid snapshot records for the fixture tree with
# deterministic 19-digit guids derived from dataset and snapshot indexes.
# Usage: Internal generator shared by the fixture-state writer. Arguments:
# <root-dataset> <dataset-count> <snap-count> <order> where order
# "creation" interleaves snapshot-major (mimics `zfs list -s creation -r`)
# and "dataset" groups dataset-major (mimics plain `zfs list -r`).
zxfer_mockbin_emit_snapshot_records() {
	l_mockbin_emit_root=$1
	l_mockbin_emit_datasets=$2
	l_mockbin_emit_snaps=$3
	l_mockbin_emit_order=$4

	awk -v root="$l_mockbin_emit_root" -v n="$l_mockbin_emit_datasets" \
		-v s="$l_mockbin_emit_snaps" -v order="$l_mockbin_emit_order" '
		function dataset_name(d) {
			return (d == 0) ? root : root "/child" d
		}
		# guid layout: "1" + 4-digit dataset idx + 5-digit snap idx +
		# 9-digit constant = 19 deterministic digits.
		function guid(d, i) {
			return sprintf("1%04d%05d%09d", d, i, 7)
		}
		BEGIN {
			if (order == "creation") {
				for (i = 1; i <= s; i++)
					for (d = 0; d <= n; d++)
						printf "%s@snap%d\t%s\n", dataset_name(d), i, guid(d, i)
			} else {
				for (d = 0; d <= n; d++)
					for (i = 1; i <= s; i++)
						printf "%s@snap%d\t%s\n", dataset_name(d), i, guid(d, i)
			}
		}
	'
}

# Purpose: Write one complete canned-zfs fixture state directory (fixture
# files plus manifest) for a given destination snapshot depth.
# Usage: Internal writer for zxfer_mockbin_build_fixture_tree. Arguments:
# <state-dir> <dataset-count> <src-snaps> <dst-snaps>.
zxfer_mockbin_write_fixture_state_dir() {
	l_mockbin_state_dir=$1
	l_mockbin_state_datasets=$2
	l_mockbin_state_src_snaps=$3
	l_mockbin_state_dst_snaps=$4

	mkdir -p "$l_mockbin_state_dir" || return 1

	zxfer_mockbin_emit_snapshot_records "$ZXFER_MOCKBIN_SOURCE_ROOT" \
		"$l_mockbin_state_datasets" "$l_mockbin_state_src_snaps" creation \
		>"$l_mockbin_state_dir/src_snapshots.list" || return 1
	# Plain `zfs list -r -t snapshot` (no -s creation) groups dataset-major;
	# this answers the fast recursive no-op proof's source identity listing.
	zxfer_mockbin_emit_snapshot_records "$ZXFER_MOCKBIN_SOURCE_ROOT" \
		"$l_mockbin_state_datasets" "$l_mockbin_state_src_snaps" dataset \
		>"$l_mockbin_state_dir/src_snapshots_dataset.list" || return 1
	zxfer_mockbin_emit_snapshot_records "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		"$l_mockbin_state_datasets" "$l_mockbin_state_dst_snaps" dataset \
		>"$l_mockbin_state_dir/dst_snapshots.list" || return 1

	# Mimic `zfs list -H <dataset>`: name, used, avail, refer, mountpoint.
	printf '%s\t96K\t1.0G\t24K\t/%s\n' "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" >"$l_mockbin_state_dir/dst_exists.list"

	{
		printf '%s\n' "$ZXFER_MOCKBIN_DEST_ROOT"
		printf '%s\n' "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT"
		l_mockbin_state_index=1
		while [ "$l_mockbin_state_index" -le "$l_mockbin_state_datasets" ]; do
			printf '%s/child%d\n' "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
				"$l_mockbin_state_index"
			l_mockbin_state_index=$((l_mockbin_state_index + 1))
		done
	} >"$l_mockbin_state_dir/dst_datasets.list"

	# Per-dataset depth-1 snapshot listings answer non-recursive (-N) batched
	# view listings and fallback rechecks for datasets outside the batched
	# view root; -R runs serve rechecks from the recursive listing above.
	l_mockbin_state_index=0
	while [ "$l_mockbin_state_index" -le "$l_mockbin_state_datasets" ]; do
		if [ "$l_mockbin_state_index" -eq 0 ]; then
			l_mockbin_state_dataset=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
		else
			l_mockbin_state_dataset="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child$l_mockbin_state_index"
		fi
		awk -v dataset="$l_mockbin_state_dataset" -v d="$l_mockbin_state_index" \
			-v s="$l_mockbin_state_dst_snaps" 'BEGIN {
				for (i = 1; i <= s; i++)
					printf "%s@snap%d\t1%04d%05d%09d\n", dataset, i, d, i, 7
			}' >"$l_mockbin_state_dir/dst_d1_$l_mockbin_state_index.list" || return 1
		l_mockbin_state_index=$((l_mockbin_state_index + 1))
	done

	{
		printf '%s\t%s\t%s\n' \
			"list -Hr -o name,guid -s creation -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT" \
			src_snapshots.list 0
		printf '%s\t%s\t%s\n' \
			"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_SOURCE_ROOT" \
			src_snapshots_dataset.list 0
		printf '%s\t%s\t%s\n' \
			"list -H $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" dst_exists.list 0
		printf '%s\t%s\t%s\n' \
			"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
			dst_snapshots.list 0
		printf '%s\t%s\t%s\n' \
			"list -t filesystem,volume -Hr -o name $ZXFER_MOCKBIN_DEST_ROOT" \
			dst_datasets.list 0
		l_mockbin_state_index=0
		while [ "$l_mockbin_state_index" -le "$l_mockbin_state_datasets" ]; do
			if [ "$l_mockbin_state_index" -eq 0 ]; then
				l_mockbin_state_dataset=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT
			else
				l_mockbin_state_dataset="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child$l_mockbin_state_index"
			fi
			printf '%s\t%s\t%s\n' \
				"list -H -d 1 -o name,guid -t snapshot $l_mockbin_state_dataset" \
				"dst_d1_$l_mockbin_state_index.list" 0
			l_mockbin_state_index=$((l_mockbin_state_index + 1))
		done
	} >"$l_mockbin_state_dir/manifest"
}

# Purpose: Generate the deterministic dataset/snapshot fixture tree plus the
# manifests covering every discovery command shape the current ./zxfer
# issues for local recursive replication.
# Usage: zxfer_mockbin_build_fixture_tree <fixture-dir> <num-datasets>
# <snaps-per-dataset>. Source tree is srcpool/data with child1..childN and
# snap1..snapS each (root included). Builds two complete state dirs to use
# as MOCK_ZFS_FIXTURE_DIR:
#   <fixture-dir>/noop         destination identical to the source;
#   <fixture-dir>/incremental  every destination dataset missing the last
#                              snapshot (requires snaps-per-dataset >= 2).
zxfer_mockbin_build_fixture_tree() {
	l_mockbin_tree_root=$1
	l_mockbin_tree_datasets=$2
	l_mockbin_tree_snaps=$3

	case "$l_mockbin_tree_datasets" in
	'' | *[!0-9]*)
		printf 'zxfer_mockbin_build_fixture_tree: num-datasets must be a non-negative integer: %s\n' \
			"$l_mockbin_tree_datasets" >&2
		return 1
		;;
	esac
	case "$l_mockbin_tree_snaps" in
	'' | *[!0-9]* | 0 | 1)
		printf 'zxfer_mockbin_build_fixture_tree: snaps-per-dataset must be >= 2: %s\n' \
			"$l_mockbin_tree_snaps" >&2
		return 1
		;;
	esac

	zxfer_mockbin_write_fixture_state_dir "$l_mockbin_tree_root/noop" \
		"$l_mockbin_tree_datasets" "$l_mockbin_tree_snaps" \
		"$l_mockbin_tree_snaps" || return 1
	zxfer_mockbin_write_fixture_state_dir "$l_mockbin_tree_root/incremental" \
		"$l_mockbin_tree_datasets" "$l_mockbin_tree_snaps" \
		"$((l_mockbin_tree_snaps - 1))" || return 1
}

# Purpose: Print the ZXFER_SECURE_PATH value that resolves mocks first and
# everything else from the standard system directories.
# Usage: zxfer_mockbin_secure_path_env <mockdir> — pass the result as
# ZXFER_SECURE_PATH when invoking ./zxfer so the canned zfs and any counting
# wrappers shadow the system tools.
zxfer_mockbin_secure_path_env() {
	l_mockbin_secure_dir=$1

	printf '%s:%s\n' "$l_mockbin_secure_dir" "$ZXFER_MOCKBIN_SYSTEM_SECURE_PATH"
}

# Purpose: Create a private bench work directory directly under /tmp, with a
# tmp/ subdirectory to pass to zxfer as TMPDIR, whatever the caller's
# TMPDIR. Path lengths change zxfer's work: remote commands embed the mock
# bin path, and a script past 768 bytes is rendered a second time in
# chunks; and a TMPDIR past about 50 bytes pushes control sockets over the
# ~104-byte sun_path limit, so zxfer takes a socket-directory fallback that
# adds helper spawns and time. A short fixed parent keeps counts and
# timings the same on every host.
# Usage: zxfer_mockbin_make_bench_workdir <name-prefix>; prints the
# directory, or returns 1. The caller removes it.
zxfer_mockbin_make_bench_workdir() {
	l_mockbin_workdir=$(mktemp -d "/tmp/$1.XXXXXX") || return 1
	[ -d "$l_mockbin_workdir" ] || return 1
	mkdir "$l_mockbin_workdir/tmp" || {
		rm -rf "$l_mockbin_workdir"
		return 1
	}
	printf '%s\n' "$l_mockbin_workdir"
}

# Purpose: Run the real ./zxfer launcher black-box against a mock bin dir and
# one canned-zfs fixture state directory.
# Usage: zxfer_mockbin_run_zxfer <mockdir> <fixture-state-dir> <zfs-log>
# [zxfer-arg...]. Resolves the launcher from $ZXFER_MOCKBIN_ZXFER_BIN, then
# $ZXFER_ROOT/zxfer (set by tests/test_helper.sh). Refuses to run when the
# canned zfs is missing from <mockdir> so a misbuilt mock dir can never let
# zxfer resolve a real zfs. Stdout/stderr pass through; redirect at the call
# site. Returns zxfer's exit status.
# Side effects: Runs the launcher with a fixture-scoped secure PATH and canned
# ZFS state while leaving the caller's environment unchanged.
zxfer_mockbin_run_zxfer() {
	l_mockbin_run_mockdir=$1
	l_mockbin_run_state=$2
	l_mockbin_run_log=$3
	shift 3

	l_mockbin_run_bin=${ZXFER_MOCKBIN_ZXFER_BIN:-${ZXFER_ROOT:-.}/zxfer}
	if [ ! -x "$l_mockbin_run_bin" ]; then
		printf 'zxfer_mockbin_run_zxfer: zxfer launcher not executable: %s\n' \
			"$l_mockbin_run_bin" >&2
		return 1
	fi
	if [ ! -x "$l_mockbin_run_mockdir/zfs" ]; then
		printf 'zxfer_mockbin_run_zxfer: canned zfs missing in %s; refusing to run zxfer\n' \
			"$l_mockbin_run_mockdir" >&2
		return 1
	fi

	MOCK_ZFS_LOG="$l_mockbin_run_log" \
		MOCK_ZFS_FIXTURE_DIR="$l_mockbin_run_state" \
		ZXFER_SECURE_PATH=$(zxfer_mockbin_secure_path_env "$l_mockbin_run_mockdir") \
		ZXFER_SECURE_PATH_APPEND="" \
		"$l_mockbin_run_bin" "$@"
}
