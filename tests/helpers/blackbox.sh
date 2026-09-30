#!/bin/sh
#
# Shared helpers for black-box suites that drive the real ./zxfer launcher
# against the canned zfs from tests/mock_toolchain_helper.sh and assert on the
# MOCK_ZFS_LOG argv, the exit status, and stderr.
#
# A suite entry sources tests/test_helper.sh, then this file, defines its
# test_* functions, and finally sources shunit2. This file supplies the shunit2
# lifecycle hooks: every case gets a private CASE_DIR, removed afterwards.
# Helpers publish their paths in MOCKBIN_DIR, FIXTURE_DIR, ZFS_LOG, STATE_DIR,
# JOB_TMP_DIR, SSH_LOG, ARGV_LOG, PARALLEL_ARGV_LOG, and the backup-mode
# variables named where they are set.
#
# shellcheck disable=SC1090,SC2034,SC2154

# shellcheck source=tests/mock_toolchain_helper.sh
. "$ZXFER_ROOT/tests/mock_toolchain_helper.sh"

# The fixture roots every case starts from (see planning_use_fixture_roots).
planning_default_source_root=$ZXFER_MOCKBIN_SOURCE_ROOT
planning_default_dest_root=$ZXFER_MOCKBIN_DEST_ROOT
planning_default_dest_mapped_root=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT

oneTimeSetUp() {
	zxfer_test_create_tmpdir "zxfer_blackbox"
}

oneTimeTearDown() {
	zxfer_test_cleanup_tmpdir
}

setUp() {
	unset MOCK_ZFS_LOG MOCK_ZFS_FIXTURE_DIR MOCK_ZFS_DEFAULT_STATUS \
		MOCK_SPAWN_LOG ZXFER_BACKUP_DIR
	planning_use_fixture_roots "$planning_default_source_root" \
		"$planning_default_dest_root" "$planning_default_dest_mapped_root"
	CASE_DIR=$(mktemp -d "$TEST_TMPDIR/case.XXXXXX") ||
		fail "Unable to create per-case temp directory."
}

tearDown() {
	if [ -n "${CASE_DIR:-}" ]; then
		rm -rf "$CASE_DIR"
	fi
	CASE_DIR=""
}

# ---------------------------------------------------------------------------
# Environment, runs, and argv-log assertions.

# Purpose: Point the fixture builders and the planning_* helpers at other
# dataset roots for the rest of the case; setUp restores the defaults.
# Usage: planning_use_fixture_roots <source-root> <destination-root>
# <mapped-destination-root>
planning_use_fixture_roots() {
	ZXFER_MOCKBIN_SOURCE_ROOT=$1
	ZXFER_MOCKBIN_DEST_ROOT=$2
	ZXFER_MOCKBIN_DEST_MAPPED_ROOT=$3
}

# Purpose: Build the standard environment: canned zfs in MOCKBIN_DIR plus the
# 2-child x 3-snapshot fixture tree in FIXTURE_DIR ("noop" and "incremental").
# Usage: planning_setup_env
planning_setup_env() {
	MOCKBIN_DIR="$CASE_DIR/mockbin"
	FIXTURE_DIR="$CASE_DIR/fixtures"
	ZFS_LOG="$CASE_DIR/zfs.log"

	mkdir -p "$MOCKBIN_DIR" || fail "Unable to create mock bin directory."
	zxfer_mockbin_write_canned_zfs "$MOCKBIN_DIR/zfs" ||
		fail "Unable to write canned zfs."
	zxfer_mockbin_build_fixture_tree "$FIXTURE_DIR" 2 3 ||
		fail "Unable to build fixture tree."
}

# Purpose: Run ./zxfer against one fixture state dir, capturing stdout and
# stderr in $CASE_DIR/zxfer.stdout and $CASE_DIR/zxfer.stderr.
# Usage: planning_run_zxfer <state-dir> [zxfer-arg...]; returns zxfer's status.
planning_run_zxfer() {
	l_state_dir=$1
	shift

	zxfer_mockbin_run_zxfer "$MOCKBIN_DIR" "$l_state_dir" "$ZFS_LOG" "$@" \
		>"$CASE_DIR/zxfer.stdout" 2>"$CASE_DIR/zxfer.stderr"
}

# Purpose: Copy a fixture state dir into a case-local STATE_DIR that a test may
# rewrite without touching the generated tree.
# Usage: planning_clone_state <source-state-dir> <name>
planning_clone_state() {
	l_clone_source=$1
	l_clone_name=$2

	STATE_DIR="$CASE_DIR/state_$l_clone_name"
	mkdir -p "$STATE_DIR" || fail "Unable to create scratch state directory."
	cp "$l_clone_source"/* "$STATE_DIR/" ||
		fail "Unable to clone fixture state directory."
}

# Purpose: Make one exact manifest key in STATE_DIR answer with no output and
# a non-zero status.
# Usage: planning_force_manifest_failure <argv-key> <status>
planning_force_manifest_failure() {
	l_force_key=$1
	l_force_status=$2

	awk -F'\t' -v key="$l_force_key" -v status="$l_force_status" '
		BEGIN { OFS = "\t" }
		$1 == key { print $1, "-", status; next }
		{ print }
	' "$STATE_DIR/manifest" >"$STATE_DIR/manifest.new" ||
		fail "Unable to rewrite manifest rule."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install rewritten manifest."
}

# Purpose: Wrap the canned zfs so a receive into a matching dataset sleeps
# first, keeping that receive in flight for ordering or signal tests.
# Usage: planning_delay_canned_zfs_receive <dataset-glob> <seconds>; the glob
# is a sh case pattern matched against the receive's last argument.
planning_delay_canned_zfs_receive() {
	mv "$MOCKBIN_DIR/zfs" "$MOCKBIN_DIR/zfs.canned" ||
		fail "Unable to stage the canned zfs behind the delaying wrapper."
	cat >"$MOCKBIN_DIR/zfs" <<EOF
#!/bin/sh
case "\${1:-}" in
receive | recv)
	for delay_arg in "\$@"; do
		delay_dataset=\$delay_arg
	done
	case "\$delay_dataset" in
	$1) sleep $2 ;;
	esac
	;;
esac
exec "$MOCKBIN_DIR/zfs.canned" "\$@"
EOF
	chmod +x "$MOCKBIN_DIR/zfs"
}

# Purpose: Wrap the canned zfs so each call also appends its arguments, each
# in brackets ("[list] ... [my data] "), to ARGV_LOG: the argument boundaries
# the space-joined zfs log cannot show.
# Usage: planning_log_canned_zfs_argv; sets ARGV_LOG.
planning_log_canned_zfs_argv() {
	ARGV_LOG="$CASE_DIR/zfs_argv.log"
	mv "$MOCKBIN_DIR/zfs" "$MOCKBIN_DIR/zfs.canned" ||
		fail "Unable to stage the canned zfs behind the argv-logging wrapper."
	cat >"$MOCKBIN_DIR/zfs" <<EOF
#!/bin/sh
argv_line=""
for argv_arg in "\$@"; do
	argv_line="\$argv_line[\$argv_arg] "
done
printf '%s\n' "\$argv_line" >>"$ARGV_LOG"
exec "$MOCKBIN_DIR/zfs.canned" "\$@"
EOF
	chmod +x "$MOCKBIN_DIR/zfs"
}

# Purpose: Fail unless the zfs log holds the exact argv line.
# Usage: planning_assert_log_has_line <argv-line>
planning_assert_log_has_line() {
	l_expected_line=$1

	grep -Fx "$l_expected_line" "$ZFS_LOG" >/dev/null 2>&1 ||
		fail "Expected zfs log line missing: $l_expected_line
zfs log: $(cat "$ZFS_LOG" 2>/dev/null)"
}

# Purpose: Fail when the zfs log holds any MUTATE line.
# Usage: planning_assert_no_mutations
planning_assert_no_mutations() {
	if grep -q '^MUTATE ' "$ZFS_LOG" 2>/dev/null; then
		fail "Expected zero MUTATE lines in zfs log: $(cat "$ZFS_LOG")"
	fi
}

# Purpose: Fail when the zfs log holds any send or receive line.
# Usage: planning_assert_no_send_receive
planning_assert_no_send_receive() {
	if grep -Eq '^(send|receive) ' "$ZFS_LOG" 2>/dev/null; then
		fail "Expected zero send/receive lines in zfs log: $(cat "$ZFS_LOG")"
	fi
}

# Purpose: Print the 1-based line number of the first exact match of an argv
# line in the zfs log, or nothing. Exact matching avoids prefix collisions such
# as "receive .../data" vs ".../data/child1".
# Usage: planning_log_line_number <argv-line>
planning_log_line_number() {
	awk -v needle="$1" '$0 == needle { print NR; exit }' "$ZFS_LOG"
}

# Purpose: Count ssh invocations in SSH_LOG whose rendered remote script
# contains a marker. Long scripts cross csh/tcsh as single-quoted `d` data
# chunks; only the fixed boundary between adjacent chunks is removed, so a
# marker split at byte 128 stays visible without evaluating the command.
# Usage: planning_count_remote_script_marker <marker>
planning_count_remote_script_marker() {
	l_marker=$1
	awk -v marker="$l_marker" '
		BEGIN {
			quote = sprintf("%c", 39)
			data_boundary = quote " " quote "d"
		}
		{
			line = $0
			while ((position = index(line, data_boundary)) != 0)
				line = substr(line, 1, position - 1) \
					substr(line, position + length(data_boundary))
			if (index(line, marker) != 0) count++
		}
		END { print count + 0 }
	' "$SSH_LOG"
}

# Purpose: Fail unless stderr holds a runtime-class structured failure report
# with the given stage and message fragment.
# Usage: planning_assert_failure_report <stage> <message-fragment>
planning_assert_failure_report() {
	l_report_stage=$1
	l_report_message=$2

	for l_report_line in \
		"zxfer: failure report begin" \
		"zxfer: failure report end" \
		"failure_class: runtime" \
		"failure_stage: $l_report_stage"; do
		grep -Fq "$l_report_line" "$CASE_DIR/zxfer.stderr" ||
			fail "Missing failure report line: $l_report_line
stderr: $(cat "$CASE_DIR/zxfer.stderr")"
	done
	grep -Fq "$l_report_message" "$CASE_DIR/zxfer.stderr" ||
		fail "Missing failure report message: $l_report_message
stderr: $(cat "$CASE_DIR/zxfer.stderr")"
}

# ---------------------------------------------------------------------------
# Destination snapshot fixtures.

# Purpose: Give the destination root in STATE_DIR one extra snapshot (@snap9,
# guid unknown to the source, created 1700000009 = Nov 2023) in the recursive
# and depth-1 listings, plus the creation-time rule -d planning issues.
# Usage: planning_add_extra_destination_snapshot
planning_add_extra_destination_snapshot() {
	for l_extradst_fixture in dst_snapshots.list dst_d1_0.list; do
		printf '%s@snap9\t9999900009000000007\n' "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
			>>"$STATE_DIR/$l_extradst_fixture" ||
			fail "Unable to append extra snapshot to $l_extradst_fixture."
	done
	printf '%s@snap3\t1700000003\n%s@snap9\t1700000009\n' \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		>"$STATE_DIR/dst_creation.list" ||
		fail "Unable to write creation-time fixture."
	printf 'get -H -o name,value -p creation %s@*\tdst_creation.list\t0\n' \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" >>"$STATE_DIR/manifest" ||
		fail "Unable to append creation-time manifest rule."
}

# Purpose: Diverge the destination root's newest snapshot in STATE_DIR (same
# name @snap3, guid 9999900003000000007) in the recursive and depth-1
# listings, plus the creation-time rule for the destroy candidate and the
# last-common rollback anchor.
# Usage: planning_make_destination_diverged
planning_make_destination_diverged() {
	for l_diverge_fixture in dst_snapshots.list dst_d1_0.list; do
		awk -F'\t' 'BEGIN { OFS = "\t" }
			$1 == "dstpool/back/data@snap3" { $2 = "9999900003000000007" }
			{ print }
		' "$FIXTURE_DIR/noop/$l_diverge_fixture" \
			>"$STATE_DIR/$l_diverge_fixture" ||
			fail "Unable to rewrite $l_diverge_fixture with divergent guid."
	done
	printf '%s@snap2\t1700000002\n%s@snap3\t1700000003\n' \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		>"$STATE_DIR/dst_creation.list" ||
		fail "Unable to write creation-time fixture."
	printf 'get -H -o name,value -p creation %s@*\tdst_creation.list\t0\n' \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" >>"$STATE_DIR/manifest" ||
		fail "Unable to append creation-time manifest rule."
}

# Purpose: Diverge the destination root like planning_make_destination_diverged,
# but heal it at convergence: every listing answers what the real pool would
# hold at that moment.
# Usage: planning_make_destination_diverged_until_receive
#
# Two consumable 'once' rules serve the diverged recursive listing before
# convergence (today one lookup: the fast no-op proof's listing, which
# discovery reuses); no recursive listing runs after it. The root's destroy
# changes it, so its pre-send re-plan (between the destroy and the rollback)
# reads the post-destroy snap1+snap2 rows through one 'once' rule, and the
# post-receive verification falls through to the aligned depth-1 fixture the
# convergence receive would have produced.
planning_make_destination_diverged_until_receive() {
	planning_make_destination_diverged
	mv "$STATE_DIR/dst_snapshots.list" "$STATE_DIR/dst_snapshots_diverged.list" ||
		fail "Unable to stage the diverged destination listing fixture."
	cp "$FIXTURE_DIR/noop/dst_snapshots.list" "$STATE_DIR/dst_snapshots.list" ||
		fail "Unable to restore the aligned destination listing fixture."
	cp "$FIXTURE_DIR/noop/dst_d1_0.list" "$STATE_DIR/dst_d1_0.list" ||
		fail "Unable to restore the aligned depth-1 root listing fixture."
	grep -v '@snap3' "$FIXTURE_DIR/noop/dst_d1_0.list" \
		>"$STATE_DIR/dst_d1_0_post_destroy.list" ||
		fail "Unable to stage the post-destroy depth-1 root listing fixture."
	awk -F'\t' \
		-v key="list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" \
		-v d1_key="list -H -d 1 -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" '
		BEGIN { OFS = "\t" }
		$1 == key {
			print key, "dst_snapshots_diverged.list", 0, "once"
			print key, "dst_snapshots_diverged.list", 0, "once"
		}
		$1 == d1_key {
			print d1_key, "dst_d1_0_post_destroy.list", 0, "once"
		}
		{ print }
	' "$STATE_DIR/manifest" >"$STATE_DIR/manifest.new" ||
		fail "Unable to stage the consumable diverged listing rules."
	mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
		fail "Unable to install the consumable diverged listing rules."
}

# ---------------------------------------------------------------------------
# Parallel (-j) fixtures.

# Purpose: Write a minimal GNU-parallel stand-in that skips options through
# "--" and runs the command once per stdin line. Like GNU parallel it replaces
# each {} with the line single-quoted for the shell (or appends it when the
# command has no {}), so a pre-quoted '{}' splits a name with spaces here as
# it does there. Sequential execution is a valid serialization of parallel's
# interleaving, so canned-zfs fixtures stay deterministic; each job gets
# stdin from /dev/null, like the real helper.
# Usage: planning_write_mock_parallel <path>
planning_write_mock_parallel() {
	l_parallel_path=$1

	cat >"$l_parallel_path" <<'EOF'
#!/bin/sh
while [ $# -gt 0 ]; do
	case "$1" in
	--)
		shift
		break
		;;
	*)
		shift
		;;
	esac
done
mock_parallel_cmd=$*
mock_parallel_q="'"
mock_parallel_status=0
while IFS= read -r mock_parallel_line || [ -n "$mock_parallel_line" ]; do
	[ -n "$mock_parallel_line" ] || continue
	# Quote the line: each ' becomes '\'' inside single quotes.
	mock_parallel_rest=$mock_parallel_line
	mock_parallel_quoted=$mock_parallel_q
	while :; do
		case $mock_parallel_rest in
		*"$mock_parallel_q"*)
			mock_parallel_head=${mock_parallel_rest%%"$mock_parallel_q"*}
			mock_parallel_quoted="$mock_parallel_quoted$mock_parallel_head$mock_parallel_q\\$mock_parallel_q$mock_parallel_q"
			mock_parallel_rest=${mock_parallel_rest#*"$mock_parallel_q"}
			;;
		*)
			mock_parallel_quoted="$mock_parallel_quoted$mock_parallel_rest$mock_parallel_q"
			break
			;;
		esac
	done
	# Substitute every {} with the quoted line.
	mock_parallel_rest=$mock_parallel_cmd
	mock_parallel_job=""
	while :; do
		case $mock_parallel_rest in
		*"{}"*)
			mock_parallel_head=${mock_parallel_rest%%"{}"*}
			mock_parallel_job="$mock_parallel_job$mock_parallel_head$mock_parallel_quoted"
			mock_parallel_rest=${mock_parallel_rest#*"{}"}
			;;
		*)
			mock_parallel_job="$mock_parallel_job$mock_parallel_rest"
			break
			;;
		esac
	done
	[ "$mock_parallel_job" != "$mock_parallel_cmd" ] ||
		mock_parallel_job="$mock_parallel_cmd $mock_parallel_quoted"
	sh -c "$mock_parallel_job" </dev/null || mock_parallel_status=$?
done
exit $mock_parallel_status
EOF
	chmod +x "$l_parallel_path"
}

# Purpose: Add the parallel source-discovery answers a -j run issues to
# STATE_DIR: the dataset enumeration plus one depth-1 creation-ordered
# snapshot listing per source dataset.
# Usage: planning_add_parallel_source_discovery_fixtures
planning_add_parallel_source_discovery_fixtures() {
	{
		printf '%s\n' "$ZXFER_MOCKBIN_SOURCE_ROOT"
		printf '%s/child1\n' "$ZXFER_MOCKBIN_SOURCE_ROOT"
		printf '%s/child2\n' "$ZXFER_MOCKBIN_SOURCE_ROOT"
	} >"$STATE_DIR/src_datasets.list" ||
		fail "Unable to write the source dataset enumeration fixture."
	printf 'list -Hr -t filesystem,volume -o name %s\tsrc_datasets.list\t0\n' \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" >>"$STATE_DIR/manifest" ||
		fail "Unable to append the source dataset enumeration manifest rule."

	l_parfix_index=0
	for l_parfix_suffix in "" /child1 /child2; do
		l_parfix_dataset="$ZXFER_MOCKBIN_SOURCE_ROOT$l_parfix_suffix"
		grep "^$l_parfix_dataset@" "$STATE_DIR/src_snapshots.list" \
			>"$STATE_DIR/src_d1_$l_parfix_index.list" ||
			fail "Unable to derive the depth-1 source listing for $l_parfix_dataset."
		printf 'list -H -o name,guid -s creation -d 1 -t snapshot %s\tsrc_d1_%s.list\t0\n' \
			"$l_parfix_dataset" "$l_parfix_index" >>"$STATE_DIR/manifest" ||
			fail "Unable to append the depth-1 source manifest rule for $l_parfix_dataset."
		l_parfix_index=$((l_parfix_index + 1))
	done
}

# Purpose: Build the shared -j environment: the incremental state cloned into
# STATE_DIR with parallel discovery answers, the mock parallel helper, and a
# private 0700 JOB_TMP_DIR to pass as TMPDIR so leftovers are observable.
# Usage: planning_setup_parallel_jobs_env <state-name>
planning_setup_parallel_jobs_env() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/incremental" "$1"
	planning_add_parallel_source_discovery_fixtures
	planning_write_mock_parallel "$MOCKBIN_DIR/parallel" ||
		fail "Unable to write the mock parallel helper."
	JOB_TMP_DIR="$CASE_DIR/jobtmp"
	mkdir -p "$JOB_TMP_DIR" || fail "Unable to create the per-run temp root."
	chmod 700 "$JOB_TMP_DIR" || fail "Unable to restrict the per-run temp root."
}

# Purpose: Fail when per-job control files or mock zfs job processes survived
# a -j run.
# Usage: planning_assert_no_parallel_job_leftovers
planning_assert_no_parallel_job_leftovers() {
	l_leftover_files=$(find "$JOB_TMP_DIR" -mindepth 1 2>/dev/null || :)
	assertEquals "the per-run temp root must hold no leftover files after exit" \
		"" "$l_leftover_files"
	# shellcheck disable=SC2009  # the assertion is about the raw process table
	l_leftover_processes=$(ps -axo command= 2>/dev/null |
		grep -F "$MOCKBIN_DIR/zfs" | grep -v grep || :)
	assertEquals "no mock zfs job processes may survive the run" \
		"" "$l_leftover_processes"
}

# ---------------------------------------------------------------------------
# Remote transport fixtures.

# Purpose: Write a mock ssh with control-master semantics: `-M` creates the
# `-S` socket file, `-O exit` removes it, and any remaining command runs
# locally through `sh -c` so helpers resolve from the inherited mock PATH.
# Argv is logged to $MOCK_SSH_LOG.
# Usage: planning_write_socket_mock_ssh <path>
planning_write_socket_mock_ssh() {
	l_ssh_path=$1

	cat >"$l_ssh_path" <<'EOF'
#!/bin/sh
[ -n "${MOCK_SSH_LOG:-}" ] && printf '%s\n' "$*" >>"$MOCK_SSH_LOG"
mock_socket=""
mock_op=""
while [ $# -gt 0 ]; do
	case "$1" in
	-M) shift ;;
	-S)
		mock_socket=$2
		shift 2
		;;
	-O)
		mock_op=$2
		shift 2
		;;
	-o) shift 2 ;;
	-*) shift ;;
	*) break ;;
	esac
done
if [ $# -gt 0 ]; then
	shift
fi
if [ "$mock_op" = exit ]; then
	rm -f "$mock_socket" 2>/dev/null
	exit 0
fi
if [ -n "$mock_socket" ] && [ ! -e "$mock_socket" ]; then
	: >"$mock_socket" 2>/dev/null || exit 255
fi
[ $# -gt 0 ] || exit 0
exec sh -c "$*"
EOF
	chmod +x "$l_ssh_path"
}

# Purpose: Assert the ssh control-master lifecycle recorded in $SSH_LOG:
# exactly MASTERS masters are opened, each on its own socket and before any
# other remote command; every later command multiplexes over one of those
# sockets; and each socket gets exactly one `-O exit`, after the last command.
# Usage: planning_assert_ssh_commands_multiplexed <masters>
planning_assert_ssh_commands_multiplexed() {
	l_mux_masters=$1
	l_mux_report=$(awk '
		# Print the word after -S, or nothing.
		function socket_of(line,    n, i, w) {
			n = split(line, w, " ")
			for (i = 1; i < n; i++)
				if (w[i] == "-S") return w[i + 1]
			return ""
		}
		$0 == "-M -V" { next }
		index($0, " -M -S ") || index($0, "-M -S ") == 1 {
			socket = socket_of($0)
			if (commands || exits) bad = bad " master-after-command:" NR
			if (socket == "" || (socket in open)) bad = bad " master-socket:" NR
			open[socket] = 1
			masters++
			next
		}
		{
			socket = socket_of($0)
			if (!(socket in open)) { bad = bad " direct:" NR; next }
			if (index($0, " -O exit ")) {
				if (socket in closed) bad = bad " second-exit:" NR
				closed[socket] = 1
				exits++
				next
			}
			if (exits) bad = bad " command-after-exit:" NR
			commands++
		}
		END {
			if (!commands) bad = bad " no-commands"
			printf "masters=%d exits=%d bad=%s\n", masters, exits, bad
		}' "$SSH_LOG")
	assertEquals "every remote command must multiplex over one of the opened masters; ssh log: $(cat "$SSH_LOG")" \
		"masters=$l_mux_masters exits=$l_mux_masters bad=" "$l_mux_report"
}

# ---------------------------------------------------------------------------
# Property pass (-P / -o / -I / -U) fixtures.

# Purpose: Seed both sides of STATE_DIR with the same default property rows,
# so the -P pass reads everything without planning any set or inherit.
# Usage: planning_add_property_transfer_fixtures
planning_add_property_transfer_fixtures() {
	l_default_rows=$(planning_property_default_rows)
	planning_add_property_fixtures_for_rows "$l_default_rows" \
		"$l_default_rows" "$l_default_rows" "$l_default_rows"
}

# Purpose: Print the default `zfs get -H` property rows: four settable local
# properties (mountpoint among them, read-only for zxfer), the dataset type,
# and the three creation-time properties every filesystem carries.
# Usage: planning_property_default_rows
planning_property_default_rows() {
	printf '%s\t%s\t%s\n' \
		type filesystem - \
		mountpoint /mnt/data local \
		compression lz4 local \
		readonly off local \
		atime off local \
		casesensitivity sensitive - \
		normalization none - \
		utf8only off -
}

# Purpose: Print a row set with one property row replaced, or appended when
# absent.
# Usage: planning_property_rows_with <rows> <name> <value> <source>
planning_property_rows_with() {
	printf '%s\n' "$1" | awk -F'\t' -v OFS='\t' -v name="$2" -v value="$3" \
		-v source="$4" '
		$1 == name { print name, value, source; seen = 1; next }
		{ print }
		END { if (!seen) print name, value, source }'
}

# Purpose: Write property fixtures with independent row sets per side and
# level, and answer every `zfs get` shape the property pass issues for the
# 2-child tree: recursive machine and human reads and name skeletons, the
# same three per-dataset fallbacks, and dataset-type probes. Recursive rows
# lead with the dataset.
# Usage: planning_add_property_fixtures_for_rows <src-root-rows>
# <src-child-rows> <dst-root-rows> <dst-child-rows>
planning_add_property_fixtures_for_rows() {
	l_src_root_rows=$1
	l_src_child_rows=$2
	l_dst_root_rows=$3
	l_dst_child_rows=$4

	{
		printf '%s\n' "$l_src_root_rows" |
			awk -v dataset="$ZXFER_MOCKBIN_SOURCE_ROOT" '{ print dataset "\t" $0 }'
		for l_prop_suffix in /child1 /child2; do
			printf '%s\n' "$l_src_child_rows" |
				awk -v dataset="$ZXFER_MOCKBIN_SOURCE_ROOT$l_prop_suffix" \
					'{ print dataset "\t" $0 }'
		done
	} >"$STATE_DIR/src_props_tree.list" ||
		fail "Unable to write source property tree fixture."
	{
		printf '%s\n' "$l_dst_root_rows" |
			awk -v dataset="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" '{ print dataset "\t" $0 }'
		for l_prop_suffix in /child1 /child2; do
			printf '%s\n' "$l_dst_child_rows" |
				awk -v dataset="$ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_prop_suffix" \
					'{ print dataset "\t" $0 }'
		done
	} >"$STATE_DIR/dst_props_tree.list" ||
		fail "Unable to write destination property tree fixture."
	printf '%s\n' "$l_src_root_rows" >"$STATE_DIR/src_props_root.list"
	printf '%s\n' "$l_src_child_rows" >"$STATE_DIR/src_props_child.list"
	printf '%s\n' "$l_dst_root_rows" >"$STATE_DIR/dst_props_root.list"
	printf '%s\n' "$l_dst_child_rows" >"$STATE_DIR/dst_props_child.list"
	# The property-name skeletons zxfer reads after each pair of value views.
	for l_prop_list in src_props_tree dst_props_tree; do
		cut -f1,2 "$STATE_DIR/$l_prop_list.list" >"$STATE_DIR/$l_prop_list.names"
	done
	for l_prop_list in src_props_root src_props_child dst_props_root dst_props_child; do
		cut -f1 "$STATE_DIR/$l_prop_list.list" >"$STATE_DIR/$l_prop_list.names"
	done
	printf 'filesystem\n' >"$STATE_DIR/type_filesystem.list"
	printf '%s\t%s\t0\n' \
		"get -r -t filesystem,volume -Ho name,property all $ZXFER_MOCKBIN_SOURCE_ROOT" src_props_tree.names \
		"get -r -t filesystem,volume -Ho name,property all $ZXFER_MOCKBIN_DEST_ROOT" dst_props_tree.names \
		"get -Ho property all $ZXFER_MOCKBIN_SOURCE_ROOT" src_props_root.names \
		"get -Ho property all $ZXFER_MOCKBIN_SOURCE_ROOT/child*" src_props_child.names \
		"get -Ho property all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" dst_props_root.names \
		"get -Ho property all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child*" dst_props_child.names \
		"get -r -t filesystem,volume -Hpo name,property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT" src_props_tree.list \
		"get -r -t filesystem,volume -Ho name,property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT" src_props_tree.list \
		"get -r -t filesystem,volume -Hpo name,property,value,source all $ZXFER_MOCKBIN_DEST_ROOT" dst_props_tree.list \
		"get -r -t filesystem,volume -Ho name,property,value,source all $ZXFER_MOCKBIN_DEST_ROOT" dst_props_tree.list \
		"get -Hpo property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT" src_props_root.list \
		"get -Ho property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT" src_props_root.list \
		"get -Hpo property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT/child*" src_props_child.list \
		"get -Ho property,value,source all $ZXFER_MOCKBIN_SOURCE_ROOT/child*" src_props_child.list \
		"get -Hpo property,value,source all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" dst_props_root.list \
		"get -Ho property,value,source all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" dst_props_root.list \
		"get -Hpo property,value,source all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child*" dst_props_child.list \
		"get -Ho property,value,source all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT/child*" dst_props_child.list \
		"get -Hpo value type $ZXFER_MOCKBIN_SOURCE_ROOT*" type_filesystem.list \
		"get -Hpo value type $ZXFER_MOCKBIN_DEST_ROOT*" type_filesystem.list \
		>>"$STATE_DIR/manifest" ||
		fail "Unable to append property manifest rules."
}

# Purpose: Answer every destination-support probe the -U scan may issue:
# source dataset types, source and destination property-name inventories, and
# per-property destination probes where "overlay" is unknown and every other
# property supported. Call after planning_add_property_fixtures_for_rows so
# its exact per-dataset rules win over the trailing per-property glob.
# Usage: planning_add_unsupported_property_fixtures <src-rows> <dst-rows>
planning_add_unsupported_property_fixtures() {
	l_unsupported_src_rows=$1
	l_unsupported_dst_rows=$2

	printf '%s\tfilesystem\n' "$ZXFER_MOCKBIN_SOURCE_ROOT" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT/child1" "$ZXFER_MOCKBIN_SOURCE_ROOT/child2" \
		>"$STATE_DIR/src_types.list"
	printf '%s\n' "$l_unsupported_src_rows" | awk -F'\t' '{ print $1 }' \
		>"$STATE_DIR/src_prop_names.list"
	printf '%s\n' "$l_unsupported_dst_rows" | awk -F'\t' '{ print $1 }' \
		>"$STATE_DIR/dst_prop_names.list"
	printf "bad property list: invalid property 'overlay'\n" \
		>"$STATE_DIR/unsupported_overlay.list"
	printf 'compression\tlz4\tlocal\n' >"$STATE_DIR/probe_ok.list"
	printf '%s\t%s\t%s\n' \
		"get -Hpo name,value type $ZXFER_MOCKBIN_SOURCE_ROOT*" src_types.list 0 \
		"get -Hpo property all $ZXFER_MOCKBIN_SOURCE_ROOT*" src_prop_names.list 0 \
		"get -Hpo property all $ZXFER_MOCKBIN_DEST_MAPPED_ROOT*" dst_prop_names.list 0 \
		"get -Hpo property,value,source overlay $ZXFER_MOCKBIN_DEST_MAPPED_ROOT*" unsupported_overlay.list 1 \
		"get -Hpo property,value,source * $ZXFER_MOCKBIN_DEST_MAPPED_ROOT*" probe_ok.list 0 \
		>>"$STATE_DIR/manifest" ||
		fail "Unable to append unsupported-property manifest rules."
}

# Purpose: Fail unless every `get` naming a destination dataset crossed the
# ssh transport and no `get` naming a source dataset did. Remote argv reaches
# SSH_LOG as single-quoted tokens, so quotes are stripped before matching.
# Usage: planning_assert_property_reads_routed_by_side
planning_assert_property_reads_routed_by_side() {
	l_routed_ssh_argv=$(tr -d "'" <"$SSH_LOG")
	l_routed_dst_gets=$(grep '^get ' "$ZFS_LOG" | grep -- " $ZXFER_MOCKBIN_DEST_ROOT" || :)
	assertNotNull "the -P pass must have read destination properties; zfs log: $(cat "$ZFS_LOG")" \
		"$l_routed_dst_gets"
	while IFS= read -r l_routed_dst_get; do
		[ -n "$l_routed_dst_get" ] || continue
		case "$l_routed_ssh_argv" in
		*"$l_routed_dst_get"*) ;;
		*)
			fail "destination property read ran locally instead of over ssh: $l_routed_dst_get
ssh log: $(cat "$SSH_LOG")"
			;;
		esac
	done <<EOF
$l_routed_dst_gets
EOF
	assertEquals "no source-side property read may cross the destination ssh transport" \
		0 "$(printf '%s\n' "$l_routed_ssh_argv" | grep -c "get .* $ZXFER_MOCKBIN_SOURCE_ROOT" || :)"
}

# Purpose: Run the property pass recursively against STATE_DIR and assert
# exit 0, leaving the zfs log ready for argv assertions.
# Usage: planning_run_property_pass [zxfer-arg...]
planning_run_property_pass() {
	planning_run_zxfer "$STATE_DIR" "$@" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_run_status=$?
	assertEquals "property pass ($*) should exit 0; stderr: $(cat "$CASE_DIR/zxfer.stderr")" \
		0 "$l_run_status"
}

# ---------------------------------------------------------------------------
# -k / -e property backup metadata fixtures.
#
# The documented layout (man/zxfer.8) puts the exact-pair file at
# ZXFER_BACKUP_DIR/<source>/.zxfer_backup_info.v2/h/<chunks>/
# .zxfer_backup_info.v2, where <chunks> is the hex of "<source>\n<destination>"
# split into 48-character components. It is derived here independently
# (od + awk) so the pins fail if the layout drifts.

# Purpose: Print the documented exact-pair metadata path for one pair.
# Usage: planning_backup_metadata_file <backup-root> <source> <destination>
planning_backup_metadata_file() {
	l_backup_key=$(printf '%s\n%s' "$2" "$3" | od -An -tx1 -v | tr -d ' \n' |
		awk '{ p = "h"; for (i = 1; i <= length($0); i += 48) p = p "/" substr($0, i, 48); print p }')
	printf '%s/%s/.zxfer_backup_info.v2/%s/.zxfer_backup_info.v2\n' "$1" "$2" "$l_backup_key"
}

# Purpose: Build the -k environment: property fixtures on a noop STATE_DIR, a
# counting `mv` wrapper (one rename per published file is the write-once pin),
# and a case-local backup root. Publishes BACKUP_ROOT, PRIMARY_FILE,
# FORWARDED_FILE, and SPAWN_LOG.
# Usage: planning_setup_backup_env <state-name>
planning_setup_backup_env() {
	planning_setup_env
	planning_clone_state "$FIXTURE_DIR/noop" "$1"
	planning_add_property_transfer_fixtures
	zxfer_mockbin_write_counting_wrapper "$MOCKBIN_DIR/mv" \
		"$(zxfer_mockbin_resolve_host_tool mv)" ||
		fail "Unable to write counting mv wrapper."
	BACKUP_ROOT="$CASE_DIR/backup"
	PRIMARY_FILE=$(planning_backup_metadata_file "$BACKUP_ROOT" \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT")
	FORWARDED_FILE=$(planning_backup_metadata_file "$BACKUP_ROOT" \
		"$ZXFER_MOCKBIN_DEST_MAPPED_ROOT" "$ZXFER_MOCKBIN_DEST_MAPPED_ROOT")
	SPAWN_LOG="$CASE_DIR/spawn.log"
}

# Purpose: Run ./zxfer recursively against STATE_DIR with
# ZXFER_BACKUP_DIR=$BACKUP_ROOT and a fresh spawn log armed.
# Usage: planning_run_backup_zxfer [zxfer-arg...]; returns zxfer's status.
planning_run_backup_zxfer() {
	: >"$SPAWN_LOG"
	export MOCK_SPAWN_LOG="$SPAWN_LOG" ZXFER_BACKUP_DIR="$BACKUP_ROOT"
	planning_run_zxfer "$STATE_DIR" "$@" -R \
		"$ZXFER_MOCKBIN_SOURCE_ROOT" "$ZXFER_MOCKBIN_DEST_ROOT"
	l_backup_run_status=$?
	unset MOCK_SPAWN_LOG ZXFER_BACKUP_DIR
	return "$l_backup_run_status"
}

# Purpose: Fail unless a metadata file is mode 0600 with the version-2 header
# for the given roots and exactly one relative row per dataset, and no stage
# file is left under BACKUP_ROOT.
# Usage: planning_assert_backup_file_is_current_format <file> <source-root>
# <destination-root>
planning_assert_backup_file_is_current_format() {
	l_backup_file=$1
	l_backup_source_root=$2
	l_backup_destination_root=$3

	assertTrue "backup metadata file must exist: $l_backup_file
stderr: $(cat "$CASE_DIR/zxfer.stderr")" "[ -f '$l_backup_file' ]"
	case "$(ls -ldn "$l_backup_file")" in
	-rw-------*) ;;
	*) fail "backup metadata must be mode 0600: $(ls -ldn "$l_backup_file")" ;;
	esac
	for l_backup_header_line in \
		"#zxfer property backup file" \
		"#format_version:2" \
		"#source_root:$l_backup_source_root" \
		"#destination_root:$l_backup_destination_root"; do
		grep -Fxq "$l_backup_header_line" "$l_backup_file" ||
			fail "backup metadata header line missing: $l_backup_header_line
file: $(cat "$l_backup_file")"
	done
	assertEquals "the first line must be the header" \
		"#zxfer property backup file" "$(sed -n '1p' "$l_backup_file")"
	for l_backup_row_key in . child1 child2; do
		assertEquals "exactly one relative row for $l_backup_row_key" \
			1 "$(grep -c "^$l_backup_row_key	" "$l_backup_file")"
		grep -q "^$l_backup_row_key	.*compression=lz4=local" "$l_backup_file" ||
			fail "row $l_backup_row_key must record the source property; file: $(cat "$l_backup_file")"
	done
	assertEquals "no stage files may be left behind under the backup root" \
		"" "$(find "$BACKUP_ROOT" -name '.zxfer-backup-write.*' 2>/dev/null)"
}

# ---------------------------------------------------------------------------
# -m / -c service manager fixture.

# Purpose: Write a mock SMF svcadm that appends "svcadm <argv>" to MOCK_ZFS_LOG,
# so the log orders every service change against the zfs calls, and fails
# (status 1, with a stderr note) each call whose space-joined argv matches the
# sh glob in MOCK_SVCADM_FAIL. Placed in MOCKBIN_DIR, it shadows the real
# svcadm of an illumos host, which a test must never run.
# Usage: planning_write_mock_svcadm <path>
planning_write_mock_svcadm() {
	cat >"$1" <<'EOF'
#!/bin/sh
[ -z "${MOCK_ZFS_LOG:-}" ] || printf 'svcadm %s\n' "$*" >>"$MOCK_ZFS_LOG"
if [ -n "${MOCK_SVCADM_FAIL:-}" ]; then
	case "$*" in
	$MOCK_SVCADM_FAIL)
		printf 'svcadm: mock failure: %s\n' "$*" >&2
		exit 1
		;;
	esac
fi
exit 0
EOF
	chmod +x "$1"
}

# Purpose: Write the forwarded alias an earlier -k hop leaves for ROOT (the
# current-format file keyed ROOT/ROOT) under BACKUP_ROOT: 0700 directories,
# a 0600 file, and the given relative rows.
# Usage: planning_write_backup_alias <root> <row>...
planning_write_backup_alias() {
	l_alias_root=$1
	shift
	l_alias_file=$(planning_backup_metadata_file "$BACKUP_ROOT" "$l_alias_root" \
		"$l_alias_root")
	(umask 077 && mkdir -p "${l_alias_file%/*}") ||
		fail "Unable to create the alias directory of $l_alias_root."
	printf '%s\n' "#zxfer property backup file" "#format_version:2" \
		"#source_root:$l_alias_root" "#destination_root:$l_alias_root" "$@" \
		>"$l_alias_file" || fail "Unable to write the alias of $l_alias_root."
	chmod 600 "$l_alias_file"
}

# Purpose: Run planning_run_backup_zxfer for an -O or -T case: the mock ssh
# logs to a fresh SSH_LOG, and PATH leads with MOCKBIN_DIR in a subshell so
# it never leaks into the suite.
# Usage: planning_run_backup_zxfer_over_ssh [zxfer-arg...]; needs SSH_LOG
# and a mock ssh in MOCKBIN_DIR. Returns zxfer's status.
planning_run_backup_zxfer_over_ssh() {
	: >"$SSH_LOG"
	export MOCK_SSH_LOG="$SSH_LOG"
	(
		PATH=$(zxfer_mockbin_secure_path_env "$MOCKBIN_DIR")
		export PATH
		planning_run_backup_zxfer "$@"
	)
	l_over_ssh_status=$?
	unset MOCK_SSH_LOG
	return "$l_over_ssh_status"
}

# ---------------------------------------------------------------------------
# Listing fault and codec fixtures.

# Purpose: Make one exact manifest key in STATE_DIR succeed with no output: a
# listing that finds nothing.
# Usage: planning_answer_manifest_key_with_nothing <argv-key>
planning_answer_manifest_key_with_nothing() {
	planning_force_manifest_failure "$1" 0
}

# Purpose: Wrap the canned zfs so the first call whose space-joined argv is
# exactly ARGV answers from the manifest as usual and then prints STDERR and
# exits STATUS: a listing that dies after streaming its rows, or with STATUS
# 0 one that succeeds with a warning. Later calls with that argv answer
# normally. Calling the helper again re-arms the wrapper with new values.
# Usage: planning_fail_canned_zfs_after_output <argv> <status> [<stderr>]
planning_fail_canned_zfs_after_output() {
	l_after_dir="$CASE_DIR/after_output"
	mkdir -p "$l_after_dir" ||
		fail "Unable to create the after-output wrapper state."
	printf '%s\n' "$1" >"$l_after_dir/argv" ||
		fail "Unable to arm the after-output wrapper."
	printf '%s\n' "$2" >"$l_after_dir/status" ||
		fail "Unable to arm the after-output wrapper."
	: >"$l_after_dir/stderr" || fail "Unable to arm the after-output wrapper."
	if [ -n "${3:-}" ]; then
		printf '%s\n' "$3" >"$l_after_dir/stderr" ||
			fail "Unable to write the after-output stderr."
	fi
	rm -f "$l_after_dir/fired"
	[ ! -f "$MOCKBIN_DIR/zfs.answering" ] || return 0
	mv "$MOCKBIN_DIR/zfs" "$MOCKBIN_DIR/zfs.answering" ||
		fail "Unable to stage the canned zfs behind the after-output wrapper."
	cat >"$MOCKBIN_DIR/zfs" <<WRAPPER
#!/bin/sh
IFS= read -r after_argv <"$l_after_dir/argv"
# The first matching call claims the marker; noclobber makes the claim
# atomic when the no-op proof lists both sides at once.
if [ "\$*" = "\$after_argv" ] &&
	(set -C && printf '' >"$l_after_dir/fired") 2>/dev/null; then
	"$MOCKBIN_DIR/zfs.answering" "\$@"
	cat "$l_after_dir/stderr" >&2
	IFS= read -r after_status <"$l_after_dir/status"
	exit "\$after_status"
fi
exec "$MOCKBIN_DIR/zfs.answering" "\$@"
WRAPPER
	chmod +x "$MOCKBIN_DIR/zfs" ||
		fail "Unable to make the after-output wrapper executable."
}

# Purpose: Write a zstd stand-in whose "compression" prefixes every line with
# "zstd:" and whose -d strips that prefix again, failing on a line without
# it, so a stream that skips either side of the codec no longer parses. Each
# call appends its arguments to <path>.argv.
# Usage: planning_write_mock_zstd <path>
planning_write_mock_zstd() {
	cat >"$1" <<EOF_ZSTD
#!/bin/sh
printf '%s\n' "\$*" >>"$1.argv"
for mock_zstd_arg in "\$@"; do
	[ "\$mock_zstd_arg" = -d ] || continue
	exec awk 'substr(\$0, 1, 5) != "zstd:" { bad = 1; next }
		{ print substr(\$0, 6) }
		END { exit bad }'
done
exec sed 's/^/zstd:/'
EOF_ZSTD
	chmod +x "$1"
}

# Purpose: Wrap the mock parallel so each call also appends its arguments,
# space-joined, to PARALLEL_ARGV_LOG, locally and on a mock-ssh origin alike.
# Usage: planning_log_mock_parallel_argv, after planning_setup_parallel_jobs_env;
# sets PARALLEL_ARGV_LOG.
planning_log_mock_parallel_argv() {
	PARALLEL_ARGV_LOG="$CASE_DIR/parallel.argv"
	mv "$MOCKBIN_DIR/parallel" "$MOCKBIN_DIR/parallel.mock" ||
		fail "Unable to stage the mock parallel behind its argv log."
	cat >"$MOCKBIN_DIR/parallel" <<EOF_PARALLEL
#!/bin/sh
printf '%s\n' "\$*" >>"$PARALLEL_ARGV_LOG"
exec "$MOCKBIN_DIR/parallel.mock" "\$@"
EOF_PARALLEL
	chmod +x "$MOCKBIN_DIR/parallel" ||
		fail "Unable to make the parallel argv log executable."
}

# Purpose: Make the -T destination root and its mapped datasets answer like
# missing datasets, while the pool rule is left to the caller. The recursive
# snapshot listing fails, each exact probe prints zfs's missing-dataset line
# (probes read stdout and stderr together) and the dataset inventory fails
# once with that line on stderr, through the zfs fault injector.
# Usage: planning_make_remote_destination_root_missing; exports the MOCK_FAIL_*
# variables, which the caller unsets.
# shellcheck disable=SC2089,SC2090  # the quotes are part of the zfs message
planning_make_remote_destination_root_missing() {
	planning_force_manifest_failure \
		"list -Hr -o name,guid -t snapshot $ZXFER_MOCKBIN_DEST_MAPPED_ROOT" 1
	for l_missing_suffix in "" /child1 /child2; do
		l_missing_dataset=$ZXFER_MOCKBIN_DEST_MAPPED_ROOT$l_missing_suffix
		l_missing_fixture="missing_${l_missing_suffix#/}.list"
		printf "cannot open '%s': dataset does not exist\n" "$l_missing_dataset" \
			>"$STATE_DIR/$l_missing_fixture" ||
			fail "Unable to write the missing-dataset fixture."
		# The first matching rule wins, so these go first.
		{
			printf 'list -H %s\t%s\t1\n' "$l_missing_dataset" "$l_missing_fixture"
			cat "$STATE_DIR/manifest"
		} >"$STATE_DIR/manifest.new" ||
			fail "Unable to prepend the missing-dataset rule."
		mv "$STATE_DIR/manifest.new" "$STATE_DIR/manifest" ||
			fail "Unable to install the missing-dataset rule."
	done
	mkdir -p "$CASE_DIR/fail_calls" || fail "Unable to create the fault counter."
	MOCK_FAIL_TOOL=zfs
	MOCK_FAIL_CALL=1
	MOCK_FAIL_DIR="$CASE_DIR/fail_calls"
	MOCK_FAIL_MATCH="list -t filesystem,volume -Hr -o name $ZXFER_MOCKBIN_DEST_ROOT"
	MOCK_FAIL_STDERR="cannot open '$ZXFER_MOCKBIN_DEST_ROOT': dataset does not exist"
	MOCK_FAIL_STATUS=1
	export MOCK_FAIL_TOOL MOCK_FAIL_CALL MOCK_FAIL_DIR MOCK_FAIL_MATCH \
		MOCK_FAIL_STDERR MOCK_FAIL_STATUS
}
